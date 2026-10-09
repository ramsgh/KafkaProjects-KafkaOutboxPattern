import { useCallback, useEffect, useState } from "react";

async function request(path, options) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed (${response.status})`);
  }
  return response.json();
}

function formatDate(value) {
  return new Date(value).toLocaleString();
}

export default function App() {
  const [orders, setOrders] = useState([]);
  const [events, setEvents] = useState([]);
  const [name, setName] = useState("");
  const [amount, setAmount] = useState("49.99");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    try {
      const [nextOrders, nextEvents] = await Promise.all([
        request("/api/orders"),
        request("/api/events"),
      ]);
      setOrders(nextOrders);
      setEvents(nextEvents);
      setError("");
    } catch (err) {
      setError(err.message);
    }
  }, []);

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 2500);
    return () => window.clearInterval(timer);
  }, [refresh]);

  async function submitOrder(event) {
    event.preventDefault();
    setLoading(true);
    setError("");
    try {
      await request("/api/orders", {
        method: "POST",
        body: JSON.stringify({ customer_name: name, amount }),
      });
      setName("");
      await refresh();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="page">
      <header className="hero">
        <div className="eyebrow"><span className="pulse" /> LIVE PIPELINE</div>
        <h1>Transactional <span>Outbox</span></h1>
        <p className="subtitle">
          Orders commit to PostgreSQL first. A separate relay publishes outbox events to Kafka.
        </p>
        <div className="flow" aria-label="Order flow">
          <div className="flow-node"><b>01</b><span>API</span><small>atomic write</small></div>
          <i />
          <div className="flow-node"><b>02</b><span>PostgreSQL</span><small>orders + outbox</small></div>
          <i />
          <div className="flow-node"><b>03</b><span>Kafka</span><small>orders.events</small></div>
          <i />
          <div className="flow-node"><b>04</b><span>Consumer</span><small>deduplicated</small></div>
        </div>
      </header>

      {error && <div className="error" role="alert">{error}</div>}

      <section className="content-grid">
        <div className="panel form-panel">
          <div className="section-heading">
            <div><div className="eyebrow">PRODUCER</div><h2>Create an order</h2></div>
            <span className="tag">POSTGRES → OUTBOX</span>
          </div>
          <form onSubmit={submitOrder}>
            <label htmlFor="customer">Customer name</label>
            <input
              id="customer"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="e.g. Alex Morgan"
              maxLength={120}
              required
            />
            <label htmlFor="amount">Amount</label>
            <div className="amount-input"><span>$</span><input
              id="amount"
              type="number"
              min="0.01"
              step="0.01"
              value={amount}
              onChange={(event) => setAmount(event.target.value)}
              required
            /></div>
            <button type="submit" disabled={loading}>
              {loading ? "Saving…" : "Create order"} <span aria-hidden="true">→</span>
            </button>
          </form>
          <p className="hint">The order and its event are saved in one database transaction.</p>
        </div>

        <div className="panel">
          <div className="section-heading">
            <div><div className="eyebrow">POSTGRESQL</div><h2>Recent orders</h2></div>
            <span className="count">{orders.length}</span>
          </div>
          {orders.length === 0 ? <p className="empty">Create an order to see it here.</p> : (
            <div className="table-wrap"><table>
              <thead><tr><th>Customer</th><th>Amount</th><th>Created</th></tr></thead>
              <tbody>{orders.map((order) => (
                <tr key={order.id}>
                  <td><strong>{order.customer_name}</strong><small className="mono">{order.id.slice(0, 8)}</small></td>
                  <td>${Number(order.amount).toFixed(2)}</td>
                  <td>{formatDate(order.created_at)}</td>
                </tr>
              ))}</tbody>
            </table></div>
          )}
        </div>
      </section>

      <section className="panel events-panel">
        <div className="section-heading">
          <div><div className="eyebrow">KAFKA → CONSUMER</div><h2>Consumed events</h2></div>
          <span className="tag live-tag"><span className="pulse" /> AUTO-REFRESH</span>
        </div>
        {events.length === 0 ? <p className="empty">Waiting for the relay and consumer to process an event…</p> : (
          <div className="event-list">{events.map((event) => (
            <article className="event" key={event.event_id}>
              <div className="event-icon">↗</div>
              <div className="event-main">
                <div className="event-title"><strong>{event.event_type}</strong><span className="status">CONSUMED</span></div>
                <code>{event.event_id}</code>
                <p>{event.payload.data.customer_name} · ${Number(event.payload.data.amount).toFixed(2)}</p>
              </div>
              <time>{formatDate(event.consumed_at)}</time>
            </article>
          ))}</div>
        )}
      </section>
      <footer>At-least-once delivery · event IDs make consumer writes idempotent</footer>
    </main>
  );
}
