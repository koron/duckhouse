((g) => {
  "use strict"

  const autoRefreshInterval = 5000;

  const d = g.document;

  const queriesTbody = d.querySelector("#queries");
  const refreshButton = d.querySelector("#refresh");
  const autoRefreshCb = d.querySelector("#auto_refresh");

  const optionAuthType = d.querySelector("#opt_authtype");
  const optionAuthValue = d.querySelector("#opt_authvalue");

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

  async function cancelQuery(ev, id) {
    const url = '/status/queries/' + encodeURIComponent(id);
    const data = { x: ev.pageX, y: ev.pageY };
    try {
      // Add "Authorization" header.
      const headers = {};
      const authType = optionAuthType.value.toLowerCase();
      const authValue = optionAuthValue.value;
      switch (authType) {
        case 'basic':
          headers['Authorization'] = 'Basic ' + btoa(authValue.trim());
          break;
        case 'bearer':
          headers['Authorization'] = 'Bearer ' + authValue.trim();
          break;
      }

      const response = await fetch(url, { method: 'DELETE', headers: headers });
      if (response.status != 204) {
        if (response.status == 401) {
          throw new Error("unauthorized (401)");
        }
        throw new Error(`error status ${response.status}`);
      }
      showToast('Canceled ID:' + id, data);
      refresh();
    } catch (err) {
      showToast('Failed: ' + err.message, {
        duration: 4000,
        backgroundColor: '#9009',
        ...data
      });
    }
  }

  g.cancelQuery = cancelQuery;

  function data2row(d) {
    const id = escapeHTML(d.ID);
    return `
      <tr>
        <td><button onclick='cancelQuery(event, "${id}")'>Cancel</button> ${id}</td>
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
