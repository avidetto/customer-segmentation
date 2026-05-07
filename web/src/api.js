export async function fetchSegmentSummary(outputTable) {
  const url = `/api/segment-summary?output_table=${encodeURIComponent(outputTable)}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error("Unable to load segment summary.");
  }
  return response.json();
}

export async function runSegmentationJob({ inputTable, outputTable, numClusters }) {
  const url = "/api/run-segmentation";
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ input_table: inputTable, output_table: outputTable, num_clusters: numClusters }),
  });
  if (!response.ok) {
    throw new Error("Unable to start segmentation job.");
  }
  return response.json();
}

export async function populateSyntheticData({ tableName, numRows = 1000 }) {
  const url = "/api/populate-synthetic-data";
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ table_name: tableName, num_rows: numRows }),
  });
  if (!response.ok) {
    throw new Error("Unable to populate synthetic data.");
  }
  return response.json();
}
