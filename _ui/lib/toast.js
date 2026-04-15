((g) => {

  const sheet = new CSSStyleSheet();
  sheet.replaceSync(`
      .toast {
        background-color: #0009;
        color: #fff;
        border-radius: 4px;
        padding: 0.625em 1em;
        z-index: 9999;
        position: fixed;
        transition: opacity 0.25s ease-in-out;
      }
    `);
  g.document.adoptedStyleSheets.push(sheet);

  async function sleep(msec) {
    await new Promise((resolve) => setTimeout(resolve, msec));
  }

  async function showToast(message, options) {
    const target = options.target ?? document.body;
    const duration = options.duration ?? 2000;

    const toast = document.createElement('div');
    toast.classList.add('toast');
    toast.textContent = message;
    toast.style.left =  options.x + "px";
    toast.style.top = options.y + "px";
    if ("backgroundColor" in options) {
      toast.style.backgroundColor = options["backgroundColor"];
    }

    target.appendChild(toast);
    await sleep(duration - 250);
    toast.style.opacity = "0";
    await sleep(250);
    target.removeChild(toast);
  }

  g.showToast = showToast;
})(this);
