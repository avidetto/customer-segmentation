async function handleApiResponse(response, defaultMessage) {
  if (response.ok) {
    return response.json();
  }

  let errorMessage = defaultMessage;
  try {
    const payload = await response.json();
    if (payload?.detail) {
      errorMessage = payload.detail;
    } else if (payload?.message) {
      errorMessage = payload.message;
    }
  } catch {
    // Ignore JSON parse errors and use default message.
  }

  throw new Error(errorMessage);
}

export async function fetchSegmentSummary(outputTable) {
  const url = `/api/segment-summary?output_table=${encodeURIComponent(outputTable)}`;
  const response = await fetch(url);
  return handleApiResponse(response, "Unable to load segment summary.");
}

export async function runSegmentationJob({ inputTable, outputTable, numClusters }) {
  const url = "/api/run-segmentation";
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ input_table: inputTable, output_table: outputTable, num_clusters: numClusters }),
  });
  return handleApiResponse(response, "Unable to start segmentation job.");
}

export async function populateSyntheticData({ tableName, numRows = 1000 }) {
  const url = "/api/populate-synthetic-data";
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ table_name: tableName, num_rows: numRows }),
  });
  return handleApiResponse(response, "Unable to populate synthetic data.");
}
