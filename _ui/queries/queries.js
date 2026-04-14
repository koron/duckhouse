((g) => {
  "use strict"

  const autoRefreshInterval = 5000;

  const d = g.document;

  const queriesTbody = d.querySelector("#queries");
  const refreshButton = d.querySelector("#refresh");
  const autoRefreshCb = d.querySelector("#auto_refresh");

  const escapeRegex = /[&<>"']/g
  const escapeMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&apos;',
  };

  function escapeHTML(s) {
    if (typeof s !== 'string') {
      return s;
    }
    return s.replace(escapeRegex, m => escapeMap[m]);
  }

  async function cancelQuery(id) {
    const url = '/status/queries/' + encodeURIComponent(id);
    try {
      // TODO: Support "Authorization" header
      const response = await fetch(url, { method: 'DELETE' });
      if (response.status != 204) {
        throw new Error(`Error status ${response.status}`);
      }
      refresh();
    } catch (err) {
      console.error(err);
    }
  }

  g.cancelQuery = cancelQuery;

  function data2row(d) {
    const id = escapeHTML(d.ID);
    return `
      <tr>
        <td><button onclick='cancelQuery("${id}")'>Cancel</button> ${id}</td>
        <td>${escapeHTML(d.ConnID)}</td>
        <td><pre><code>${escapeHTML(d.Query)}</code></pre></td>
        <td>${escapeHTML(d.Start)}</td>
        <td>${escapeHTML(d.Duration)}</td>
      </tr>
    `;
  }

  async function refresh() {
    try {
      const response = await fetch("/status/queries/")
      if (response.status != 200) {
        throw new Error(`Error status ${response.status}`);
      }
      const text = await response.text();

      const data = text
        .split(/\r?\n/)
        .filter(line => line.trim() !== "")
        .slice(0, 50) // maximum 50 lines
        .map(line => JSON.parse(line));

      if (data.length == 0) {
        queriesTbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No queries</td></tr>'
        return;
      }

      // Convert to HTML
      queriesTbody.innerHTML = data
        .map(c => `${data2row(c)}`)
        .join("\n");
    } catch (err) {
      queriesTbody.innerHTML = `<tr><td colspan="5" style="text-align: center; color: red;">${err.message}</td></tr>`;
      autoRefreshCb.checked = false;
      toggleAutoRefresh();
    }
  }

  function startAutoRefresh() {
    setInterval(() => {
      if (autoRefreshCb.checked) {
        refresh();
      }
    }, autoRefreshInterval);
  }

  function toggleAutoRefresh() {
    const checked = autoRefreshCb.checked;
    refreshButton.disabled = checked;
  }

  refreshButton.addEventListener('click', ev => refresh());
  autoRefreshCb.addEventListener('change', ev => toggleAutoRefresh());

  refresh();
  startAutoRefresh();
})(this);
