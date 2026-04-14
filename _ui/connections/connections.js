((g) => {
  "use strict"

  const autoRefreshInterval = 5000;

  const d = g.document;

  const connectionsTable = d.querySelector("#connections");
  const refreshButton = d.querySelector("#refresh");
  const autoRefresh = d.querySelector("#auto_refresh");

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

  function data2row(d) {
    const s = d.DBStats;
    return `
      <tr>
        <td>${escapeHTML(d.ID)}</td>
        <td>${s.MaxOpenConnections}</td>
        <td>${s.OpenConnections}</td>
        <td>${s.InUse}</td>
        <td>${s.Idle}</td>
        <td>${s.WaitCount}</td>
        <td>${s.WaitDuration}</td>
        <td>${s.MaxIdleClosed}</td>
        <td>${s.MaxIdleTimeClosed}</td>
        <td>${s.MaxLifetimeClosed}</td>
      </tr>
    `;
  }

  async function refresh() {
    try {
      const response = await fetch("/status/connections/")
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
        connectionsTable.innerHTML = '<tr><td colspan="10" style="text-align: center;">No connections</td></tr>'
        return;
      }

      // Convert to HTML
      connectionsTable.innerHTML = data
        .map(c => `${data2row(c)}`)
        .join("\n");
    } catch (err) {
      connectionsTable.innerHTML = `<tr><td colspan="10" style="text-align: center; color: red;">${err.message}</td></tr>`;
      autoRefresh.checked = false;
      toggleAutoRefresh();
    }
  }

  function startAutoRefresh() {
    setInterval(() => {
      if (autoRefresh.checked) {
        refresh();
      }
    }, autoRefreshInterval);
  }

  function toggleAutoRefresh() {
    const checked = autoRefresh.checked;
    refreshButton.disabled = checked;
  }

  refreshButton.addEventListener('click', ev => refresh());
  autoRefresh.addEventListener('change', ev => toggleAutoRefresh());

  refresh();
  startAutoRefresh();
})(this);
