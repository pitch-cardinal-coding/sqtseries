/* sqtseries Swagger UI bootstrap (external file: the gateway CSP forbids
   inline scripts, and all assets are vendored — no CDN). */
(function () {
  "use strict";
  function ready(fn) {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", fn);
    } else {
      fn();
    }
  }
  ready(function () {
    if (!window.SwaggerUIBundle) {
      document.getElementById("swagger-ui").textContent =
        "Swagger UI failed to load.";
      return;
    }
    window.SwaggerUIBundle({
      url: "/openapi.json",
      dom_id: "#swagger-ui",
      deepLinking: true,
      displayRequestDuration: true,
    });
  });
})();
