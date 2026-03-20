Find ALL places where volume is calculated using division like:

volume = amount / buy_price
volume = investment / price

REMOVE this logic completely.

Instead:
- Treat the user input as volume directly.
- Do NOT divide it by buy price anywhere.

Final logic must be:

volume = user_input

profit = (sell_price - buy_price) * volume

ROI = (profit / (buy_price * volume)) * 100

Also:
- Remove any old variable like amount, investment if it is being used to derive volume.
- Ensure database save also stores volume directly, not derived value.
- Ensure no helper function (like in logic.py or db.py) recomputes volume again.

Do NOT change any UI, flow, or features. Only fix volume logic.
