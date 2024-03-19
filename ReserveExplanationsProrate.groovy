package reports

import constants.Constant
import constants.movement.Type
import groovy.sql.GroovyRowResult

import java.util.stream.Collectors

class ReserveExplanationsProrate extends NotShippingProrate{

    private static final int IDEMPOTENCY_RESERVE_RANGE = 10
    private Set<String> reserveExplanationsDebtProcessed = new HashSet<>()

    private ReserveExplanationsProrate(LedgerMovementService ledgerService, ProrateMovementsHelper prorateHelper) {
        super(ledgerService, prorateHelper)
    }

    protected void process(){
        prorate()
    }

    private void prorate() {
        List<GroovyRowResult> movementsToProrate = new ArrayList<>()
        Set<String> reserveReferenceDebt = new HashSet<>()
        prorateHelper.getReserveForExplanationMovements()
                .groupBy { it.internal_reference_id }
                .each { internal_reference_id, listReserveByInternalReference ->
                    reserveExplanationsDebtProcessed = new HashSet<>()
                    movementsToProrate.addAll(listReserveByInternalReference)
                    processByInternalReference(listReserveByInternalReference, movementsToProrate, reserveReferenceDebt)
                }
        processReserveByDebt(movementsToProrate, reserveReferenceDebt)
        mapMovementsByType(movementsToProrate)
    }

    private List<GroovyRowResult>  getMirrorValueReserveMovement(List<GroovyRowResult> reservesByInternalReference) {
        double totalAmountByInternalReference = getTotalAmount(reservesByInternalReference)
        if (totalAmountByInternalReference >= 0) {
            return Collections.emptyList()
        }
        List<GroovyRowResult> reservesMirrorByInternalReference = new ArrayList<>()
        List<GroovyRowResult> movesByInternalReferenceNetsInTxn = getMovesByInternalReferenceNetsInTxn(reservesByInternalReference)
        if (movesByInternalReferenceNetsInTxn == null) return Collections.emptyList()
        double totalAmountByReference = getTotalAmount(movesByInternalReferenceNetsInTxn)
        if (totalAmountByInternalReference == (totalAmountByReference * -1)) {
            reservesMirrorByInternalReference.addAll(movesByInternalReferenceNetsInTxn)
        }
        return reservesMirrorByInternalReference
    }

    private List<GroovyRowResult> getMovesByInternalReferenceNetsInTxn(List<GroovyRowResult> reservesByInternalReference) {
        GroovyRowResult reserve = reservesByInternalReference.first()
        List<GroovyRowResult> reservesByReference = getReservesByReference((Long)reserve.release_reference_id)
        if (reservesByReference == null) return Collections.emptyList()
        reservesByReference.stream()
                .filter({it->
                    it.internal_reference_id == reserve.internal_reference_id &&
                            it.release_label.toString().contains(Constant.NETS_IN_TXN) &&
                            it.release_move_id != reserve.release_move_id
                }).collect(Collectors.toList())
    }

    private List<GroovyRowResult> getReservesByReference(Long referenceId) {
        List<GroovyRowResult> reservesByReference = prorateHelper.getReserveByReference().get(referenceId.toString())
        if (reservesByReference != null) {
            return reservesByReference
        }
        return prorateHelper.getNotShippingMovementsByReference().get(referenceId)
    }

    private void processByInternalReference(List<GroovyRowResult> reservesByInternalReference, List<GroovyRowResult> movementsToProrate, Set<String> reserveReferenceDebt ) {
        if (reservesByInternalReference == null || reservesByInternalReference.isEmpty()) return

        GroovyRowResult reserve = reservesByInternalReference.last()
        loadReserveOfDebt(reserve, reserveReferenceDebt, reserveExplanationsDebtProcessed)
        List<GroovyRowResult> mirrorValueReserveMovements = getMirrorValueReserveMovement(reservesByInternalReference)
        if (isEmptyListMovements(mirrorValueReserveMovements) || !mirrorValueReserveMovements.first().release_idempotency) return

        List<GroovyRowResult> movementsByIdempotency = getMovementsByIdempotency(mirrorValueReserveMovements.first())
        if (isEmptyListMovements(movementsByIdempotency)) return

        List<GroovyRowResult> pendingInstallmentsMovement = findPNFReserveMovements(movementsByIdempotency)
        if (!isEmptyListMovements(pendingInstallmentsMovement)){
            proratePNFReserve(reserve, movementsByIdempotency, movementsToProrate, pendingInstallmentsMovement)
            return
        }

        processExplanationsReserve(movementsByIdempotency, reserve, movementsToProrate, reserveReferenceDebt, mirrorValueReserveMovements)
    }

    private void processExplanationsReserve(List<GroovyRowResult> movementsByIdempotency, GroovyRowResult reserve, List<GroovyRowResult> movementsToProrate, Set<String> reserveReferenceDebt, List<GroovyRowResult> mirrorValueReserveMovements) {
        List<GroovyRowResult> notReservesByIdempotency = getNotReservesByIdempotency(movementsByIdempotency)
        if (isEmptyListMovements(notReservesByIdempotency)) {
            getReservesForProrateByIdempotency(movementsByIdempotency, reserve.release_move_id.toString())
                    .groupBy { it.internal_reference_id }
                    .each { internal_reference_id, listReserveByInternalReference ->
                        processByInternalReference(listReserveByInternalReference, movementsToProrate, reserveReferenceDebt)
                    }
            return
        }

        if (reserve.reserve_reason.toString().contains(Constant.RESERVE_FOR_CHARGEBACK)) {
            bindFeePaymentOfCbkRecovery(notReservesByIdempotency, (Long) reserve.release_reference_id)
        }

        addMovementsForProrate(mirrorValueReserveMovements, reserve, movementsToProrate)
        addMovementsForProrate(notReservesByIdempotency, reserve, movementsToProrate)
        bindReserveForDebtWithExplanations(notReservesByIdempotency.first(), movementsToProrate, reserveExplanationsDebtProcessed)
        reserveReferenceDebt.removeAll(reserveExplanationsDebtProcessed)
    }

    private boolean isEmptyListMovements(List<GroovyRowResult> movements) {
        return movements == null || movements.isEmpty()
    }

    private List<GroovyRowResult> getMovementsByIdempotency(GroovyRowResult mirrorReserve) {
        String idempotency = mirrorReserve.release_idempotency.toString()
        List<GroovyRowResult> movementsByIdempotency = prorateHelper.getMovementsByIdempotency().get(idempotency)
        return movementsByIdempotency.stream()
                .filter({ m -> isValidRangeDate(mirrorReserve.release_date_created.toString(), m.release_date_created.toString()) })
                .collect(Collectors.toList())
    }

    private static void bindReserveForDebtWithExplanations(GroovyRowResult notReserveByIdempotency, List<GroovyRowResult> movementsToProrate, Set<String> reserveExplanationsDebtProcessed) {
        movementsToProrate.each { it ->
            if (reserveExplanationsDebtProcessed.contains(it.release_reference_id.toString())) {
                it.release_reference_id = notReserveByIdempotency.release_reference_id
            }
        }
    }

    private static boolean isValidRangeDate(String mirrorDate, idempotencyDate) {
        long seconds = ISO8601Parser.getDifferenceInSecondsDBFormat(mirrorDate, idempotencyDate)
        return seconds.abs() <= IDEMPOTENCY_RESERVE_RANGE
    }

    private static void loadReserveOfDebt(GroovyRowResult reserve, Set<String> reserveReferenceDebt, Set<String> reserveExplanationsDebtProcessed) {
        reserve.REVIEW_PAYMENT_API = true
        if (reserve.reserve_reason?.toString() in Constant.REASON_RESERVE_BY_DEBT) {
            reserveReferenceDebt.add(reserve.release_reference_id.toString())
            reserveExplanationsDebtProcessed.add(reserve.release_reference_id.toString())
        }
    }

    private void processReserveByDebt(List<GroovyRowResult> movementsToProrate, Set<String> reserveReferenceDebt) {
        if (reserveReferenceDebt.isEmpty()) return

        Map<String, Long> mapExternalReference = ledgerService.getPaymentIdByDebtFromReservesIds(reserveReferenceDebt, prorateHelper.getUserId())

        movementsToProrate.findAll { it.release_reference_id.toString() in mapExternalReference }
                .each {
                    it.release_reference_id = mapExternalReference[it.release_reference_id.toString()]
                }
    }

    private static List<GroovyRowResult> findPNFReserveMovements(List<GroovyRowResult> movementsByIdempotency) {
        return movementsByIdempotency.findAll {ProrateMovementsHelper.isPNFMovement(it) && it.reserve_reason == Constant.RESERVE_FOR_TIME_PERIOD_PNF }
    }

    private void proratePNFReserve(GroovyRowResult reserve, List<GroovyRowResult> movementsByIdempotency, List<GroovyRowResult> movementsToProrate, List<GroovyRowResult> pendingInstallmentsMovement) {

        List<GroovyRowResult> mirrorValueReserveMovements = getMirrorValueReserveMovement(getReservesForProrateByIdempotency(movementsByIdempotency, reserve.release_move_id.toString()))
        Map feeRefundAmountAndMovement = getFeeRefundAmountAndMovements(mirrorValueReserveMovements)

        double feeRefundAvailable = getFeeRefundAvailable(pendingInstallmentsMovement, feeRefundAmountAndMovement.feeAmount)

        List<GroovyRowResult> notReserveMovements = getNotReservesByIdempotency(feeRefundAmountAndMovement.movementsByIdempotencyMirror)

        if(!notReserveMovements.isEmpty()){
            updateAmountWithExplanationFee(notReserveMovements, feeRefundAvailable)
            addMovementsForProrate(mirrorValueReserveMovements, reserve, movementsToProrate)
            addMovementsForProrate(notReserveMovements, reserve, movementsToProrate)
        }
    }

    private static void updateAmountWithExplanationFee(List<GroovyRowResult> notReserveMovements, double feeRefundAvailable) {
        def amountRefund = notReserveMovements.sum { it.release_amount }
        notReserveMovements.each { it ->
            if (it.release_type == Type.FEE) {
                it.release_amount = feeRefundAvailable
            }else {
                it.release_amount = amountRefund - feeRefundAvailable
            }
        }
    }

    private double getFeeRefundAvailable(List<GroovyRowResult> pendingInstallmentsMovement, double feeRefund = 0){
        // get original fee amount for the payment
        GroovyRowResult pnfMovement = pendingInstallmentsMovement.get(0)
        double feeOriginalPay = getTotalOriginalAmountAndMovementsByType(pnfMovement, Constant.FEE).totalAmount

        // get data about installments released and pending for the payment
        Map<String, Integer> pnfDataInstallments = prorateHelper.getPnfInstallments().get(pnfMovement.release_move_id.toString())
        int totalInstallments = pnfDataInstallments[Constant.TOTAL_INSTALLMENT]
        int pendingInstallments = pendingInstallmentsMovement.size()
        int installmentsReleased = totalInstallments - pendingInstallments

        // the fee returned to the user is calculated taking into account the released installments
        double feeReleased = (feeOriginalPay / totalInstallments) * installmentsReleased
        double feePayment = feeOriginalPay + feeRefund
        double feeRefundAvailable = feePayment - feeReleased
        feeRefundAvailable = Math.round(feeRefundAvailable * 100) / 100
        return feeRefundAvailable
    }

    private Map getFeeRefundAmountAndMovements(List<GroovyRowResult> mirrorValueReserveMovements){
        List<GroovyRowResult> movementsByIdempotencyMirror = Collections.emptyList()
        double feeRefund = 0

        if (mirrorValueReserveMovements.isEmpty()) {
            return [feeAmount: feeRefund, movementsByIdempotencyMirror: movementsByIdempotencyMirror]
        }

        GroovyRowResult firstMovement = mirrorValueReserveMovements.first()
        if (firstMovement.release_idempotency) {
            movementsByIdempotencyMirror = getMovementsByIdempotency(firstMovement.release_idempotency.toString())
            List<GroovyRowResult> movementsFeeRefund = movementsByIdempotencyMirror
                    .stream()
                    .filter { it.release_type == Constant.FEE }
                    .collect(Collectors.toList())

            feeRefund = getTotalAmount(movementsFeeRefund)
        }

        return [feeAmount: feeRefund, movementsByIdempotencyMirror: movementsByIdempotencyMirror]
    }

    private List<GroovyRowResult> getMovementsByIdempotency(String idempotency) {
        return prorateHelper.getMovementsByIdempotency().get(idempotency)
    }

    private static List<GroovyRowResult> getReservesForProrateByIdempotency(List<GroovyRowResult> movementsByIdempotency, String currentMoveId) {
        return movementsByIdempotency.stream()
                .filter({ it -> it.release_detail.toString().contains(Constant.RESERVE) &&
                        !it.reserve_reason.contains(Constant.FROM_TIME_PERIOD) &&
                        ((BigDecimal)it.release_amount).doubleValue() < 0 &&
                        it.release_move_id.toString() != currentMoveId
                })
                .collect(Collectors.toList())
    }

    private static List<GroovyRowResult> getNotReservesByIdempotency(List<GroovyRowResult> movementsByIdempotency) {
        return movementsByIdempotency.stream()
                .filter({ it -> !it.release_detail.toString().contains(Constant.RESERVE) })
                .collect(Collectors.toList())
    }

    private void bindFeePaymentOfCbkRecovery(List<GroovyRowResult> movementsByIdempotency, Long referenceId) {
        GroovyRowResult feePayment = movementsByIdempotency.stream()
                .filter({
                    it.release_detail.toString().contains(Constant.PAYMENT) && it.release_type.toString().contains(Constant.FEE)
                })
                .findFirst()
                .orElse(null)

        if (feePayment == null) return
        List<GroovyRowResult> movementByReference = prorateHelper.getNotShippingMovementsByReference().get(referenceId)

        if (movementByReference == null) return
        Long releaseMoveId = movementByReference.stream()
                .filter({
                    it.release_detail.toString().contains(Constant.CBK_RECOVEY) &&
                            it.release_label.toString().contains(Constant.LABEL_IS_CANCELLATION)
                })
                .map({
                    it.release_move_id ? it.release_move_id.toLong() : null
                })
                .findFirst()
                .orElse(null)
        if (releaseMoveId == null) return

        mapFeeOriginalToCancel(feePayment, releaseMoveId)
    }

    private static void addMovementsForProrate(List<GroovyRowResult> notReservesByIdempotency, GroovyRowResult reserve, List<GroovyRowResult> movementsToProrate) {
        assignReserveDate(notReservesByIdempotency, reserve)
        movementsToProrate.addAll(notReservesByIdempotency)
    }

    private static List<GroovyRowResult> assignReserveDate(List<GroovyRowResult> movementsToProrate, reserve) {
        movementsToProrate.each { it -> it.release_date_created = reserve.release_date_created }
    }

    static ReserveExplanationsProrate getInstance(LedgerMovementService ledgerService, ProrateMovementsHelper prorateHelper) {
        return new ReserveExplanationsProrate(ledgerService, prorateHelper)
    }
}
