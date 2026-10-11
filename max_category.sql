select
    us_state,
    tupleElement(maximum, 1) as cat_id,
    tupleElement(maximum, 2) as amount
from
(
    select
        us_state,
        argMax(tuple(cat_id, amount), tuple(amount, source_row)) as maximum
    from fraud.transactions
    group by us_state
)
order by us_state;
