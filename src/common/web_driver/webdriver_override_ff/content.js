(function () {
  const code = `
    try {
      // Override in the main world (page)
      Object.defineProperty(navigator, 'webdriver', {
        get: () => false
      });
    } catch (e) {
      // silent
    }
  `;
  const s = document.createElement('script');
  s.textContent = code;
  // document_start: the <html> element exists
  (document.documentElement || document.head).appendChild(s);
  s.parentNode.removeChild(s);
})();
