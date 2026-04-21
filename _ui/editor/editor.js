((g) => {
  "use strict"

  const d = g.document;
  const localStorage = g.localStorage;
  const storagePrefix = "duckpop_";

  const queryForm = d.querySelector("#query");
  const outputForm = d.querySelector("#output");

  d.addEventListener("keydown", (ev) => {
    // Ctrl+Enter: do query
    if (ev.ctrlKey && ev.keyCode == 13) {
      ev.preventDefault();
      doQuery();
      return false;
    }
  });

  const submitButton = d.querySelector("#submit");
  const formatButton = d.querySelector("#format");
  const minifyButton = d.querySelector("#minify");

  formatButton.addEventListener("click", () => doFormat());
  minifyButton.addEventListener("click", () => doMinify());
  submitButton.addEventListener("click", () => doQuery());

  const optionFormat = d.querySelector("#opt_format");

  const progressCover = d.querySelector('#progress-cover');

  // Handlers

  function doFormat() {
    queryForm.value = sqlFormatter.format(queryForm.value);
  }

  function doMinify() {
    queryForm.value = minify(queryForm.value);
  }

  async function doQuery() {
    progressCover.style.display = '';
    try {
      const query = queryForm.value;
      const format = optionFormat.value.toLowerCase();
      const url = '/?f=' + encodeURIComponent(format);
      // post a query
      await fetch(url, {
        mode: 'cors',
        method: 'POST',
        headers: {
          'Content-Type': 'plain/text',
        },
        body: query,
      })
        .then(r => r.text())
        .then(v => outputForm.value = v);
    } finally {
      progressCover.style.display = 'none';
    }
  }

  // Functions

  function minify(s) {
    return s.split(/\n/).
      map((s) => s.replace(/--.*$/, '')).
      map((s) => s.replace(/^ +/, '')).
      map((s) => s.replace(/ +$/, '')).
      join(' ').replace(/ *([\x21-\x2f\x3a-\x40\x5b-\x60\x7b-\x7e]) */g, '$1');
  }
})(this);
