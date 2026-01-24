# Fee Booking

Overview
--------
`FeeBookingEngine` turns crystallized performance fees into payables (liabilities) before settlement. It records bookings, supports settlement and reversible cancellations (with audit trail).

Behaviour
---------
- `book_crystallized_fees(series, booking_date, period)` computes fees per class (gain above HWM * performance_fee) and records payables.
- `settle_payable(class_id, series)` deducts cash, marks payable paid, updates HWM to post‑fee total.
- `reverse_booking(class_id, reason)` removes unpaid booking and emits an audit event (reversible only when unpaid).

Audit
-----
- Emits `fee_booking.booked`, `fee_booking.settled`, and `fee_booking.reversed` events to the audit stream.
