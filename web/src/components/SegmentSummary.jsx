export default function SegmentSummary({ data }) {
  const segments = data ?? []
  const maxCustomers = Math.max(...segments.map((segment) => segment.customer_count), 1)
  const maxSpend = Math.max(...segments.map((segment) => segment.avg_spent), 1)
  const totalCustomers = segments.reduce((sum, segment) => sum + segment.customer_count, 0)
  const averageSpend =
    segments.reduce((sum, segment) => sum + segment.avg_spent, 0) / Math.max(segments.length, 1)

  return (
    <div className="segment-results">
      {segments.length ? (
        <>
          <div className="summary-metrics">
            <div>
              <span>Total Customers</span>
              <strong>{totalCustomers.toLocaleString()}</strong>
            </div>
            <div>
              <span>Segments</span>
              <strong>{segments.length}</strong>
            </div>
            <div>
              <span>Avg. Segment Spend</span>
              <strong>${averageSpend.toFixed(2)}</strong>
            </div>
          </div>

          <div className="segment-visual">
            {segments.map((segment) => {
              const customerWidth = `${Math.max((segment.customer_count / maxCustomers) * 100, 4)}%`
              const spendWidth = `${Math.max((segment.avg_spent / maxSpend) * 100, 4)}%`

              return (
                <div className="segment-row" key={segment.segment_id}>
                  <div className="segment-row-label">
                    <strong>Segment {segment.segment_id}</strong>
                    <span>{segment.customer_count.toLocaleString()} customers</span>
                  </div>
                  <div className="bar-group" aria-label={`Segment ${segment.segment_id} visual summary`}>
                    <div className="bar-line">
                      <span>Customers</span>
                      <div className="bar-track">
                        <div className="bar-fill customers-bar" style={{ width: customerWidth }} />
                      </div>
                    </div>
                    <div className="bar-line">
                      <span>Spend</span>
                      <div className="bar-track">
                        <div className="bar-fill spend-bar" style={{ width: spendWidth }} />
                      </div>
                    </div>
                  </div>
                  <div className="segment-row-stats">
                    <span>${segment.avg_spent.toFixed(2)} avg spend</span>
                    <span>${segment.avg_order_amount.toFixed(2)} avg order</span>
                    <span>{segment.avg_recency_days.toFixed(1)} days recency</span>
                  </div>
                </div>
              )
            })}
          </div>
        </>
      ) : (
        <p>No segment data available yet.</p>
      )}
    </div>
  );
}
