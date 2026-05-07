export default function SegmentSummary({ data }) {
  return (
    <div className="segment-summary">
      {data?.length ? (
        data.map((segment) => (
          <div className="segment-card" key={segment.segment_id}>
            <h3>Segment {segment.segment_id}</h3>
            <p><strong>Customers:</strong> {segment.customer_count}</p>
            <p><strong>Avg. spend:</strong> ${segment.avg_spent.toFixed(2)}</p>
            <p><strong>Avg. order:</strong> ${segment.avg_order_amount.toFixed(2)}</p>
            <p><strong>Avg. recency:</strong> {segment.avg_recency_days.toFixed(1)} days</p>
          </div>
        ))
      ) : (
        <p>No segment data available yet.</p>
      )}
    </div>
  );
}
