import { useState } from "react";
import { fetchSegmentSummary, runSegmentationJob, populateSyntheticData } from "./api";
import SegmentSummary from "./components/SegmentSummary";

export default function App() {
  const [inputTable, setInputTable] = useState("customer_purchases")
  const [outputTable, setOutputTable] = useState("customer_segments")
  const [numClusters, setNumClusters] = useState(5)
  const [syntheticRows, setSyntheticRows] = useState(1000)
  const [loading, setLoading] = useState(false)
  const [syntheticLoading, setSyntheticLoading] = useState(false)
  const [summary, setSummary] = useState(null)
  const [error, setError] = useState(null)
  const [syntheticMessage, setSyntheticMessage] = useState(null)

  const handleRunSegmentation = async () => {
    setLoading(true)
    setError(null)
    setSyntheticMessage(null)
    try {
      await runSegmentationJob({ inputTable, outputTable, numClusters })
      const response = await fetchSegmentSummary(outputTable)
      setSummary(response)
    } catch (err) {
      setError(err.message || "Failed to run segmentation.")
    } finally {
      setLoading(false)
    }
  }

  const handlePopulateSyntheticData = async () => {
    setSyntheticLoading(true)
    setError(null)
    setSyntheticMessage(null)
    try {
      await populateSyntheticData({ tableName: inputTable, numRows: syntheticRows })
      setSyntheticMessage(`Synthetic data populated into ${inputTable}.`)
    } catch (err) {
      setError(err.message || "Failed to populate synthetic data.")
    } finally {
      setSyntheticLoading(false)
    }
  }

  return (
    <div className="app-shell">
      <header>
        <h1>Customer Segmentation</h1>
        <p>Use Delta purchase data to generate customer segments for pricing and product planning.</p>
      </header>

      <section className="panel">
        <h2>Configuration</h2>
        <label>
          Source Delta Table
          <input value={inputTable} onChange={(event) => setInputTable(event.target.value)} />
        </label>
        <label>
          Output Delta Table
          <input value={outputTable} onChange={(event) => setOutputTable(event.target.value)} />
        </label>
        <label>
          Number of Clusters
          <input type="number" min="2" max="20" value={numClusters} onChange={(event) => setNumClusters(Number(event.target.value))} />
        </label>
        <label>
          Synthetic Records
          <input type="number" min="100" max="200000" step="100" value={syntheticRows} onChange={(event) => setSyntheticRows(Number(event.target.value))} />
        </label>
        <button onClick={handlePopulateSyntheticData} disabled={syntheticLoading}>
          {syntheticLoading ? "Generating data..." : "Populate Synthetic Data"}
        </button>
        <button onClick={handleRunSegmentation} disabled={loading}>
          {loading ? "Running segmentation..." : "Run Segmentation"}
        </button>
        {error && <div className="error">{error}</div>}
        {syntheticMessage && <div className="success">{syntheticMessage}</div>}
      </section>

      {summary?.length ? (
        <section className="panel">
          <h2>Segment Summary</h2>
          <SegmentSummary data={summary} />
        </section>
      ) : (
        <section className="panel">
          <h2>Summary</h2>
          <p>Run the segmentation job to load cluster summaries and customer segment insights.</p>
        </section>
      )}
    </div>
  );
}
