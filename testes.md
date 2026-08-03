# REVENUE DAILY
revenue_date, # pakai paid_at bukan created_at
route_sk_id,
class_id,
total_tickets,
gross_revenue,
total_discount,
net_revenue,
refunded_revenue,
net_revenue_after_refund,
avg_ticket_price
updated_at

# CANCELLATION SUMMARY
booking_date,
route_sk_id,
total_ticket_id
total_created,
total_paid
total_cancelled,
total_refunded,
cancel_before_payment,
cancel_after_payment,
cancellation_rate,
lost_revenue,
avg_hours_to_cancel
updated_at

# REFUND LOSS
refund_date
route_sk_id
class_id
total_tickets_refunded
total_refund_amount
avg_refund_amount
avg_days_cancel_to_refund
avg_hours_to_refund
avg_days_created_to_refund
total_refunded_with_promo
total_refunded_with_family_flag