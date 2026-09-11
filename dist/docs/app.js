/* sqtseries docs — progressive enhancement.
   Theme toggle, dynamic header/nav/footer, on-this-page TOC, breadcrumbs,
   code copy buttons, prev/next pagination.
   All features degrade gracefully. */

(function () {
  "use strict";

  var STORAGE_KEY = "sqtseries-theme";
  var VERSION = "v0.1.0";

  var NAV_ITEMS = [
    { href: "index.html",              label: "Home" },
    { href: "quickstart.html",         label: "Quick Start" },
    { href: "configuration.html",      label: "Configuration" },
    { href: "ingestion.html",          label: "Ingestion" },
    { href: "queries.html",            label: "Queries" },
    { href: "streaming.html",          label: "Streaming" },
    { href: "dashboard.html",          label: "Dashboard" },
    { href: "camera.html",             label: "Camera" },
    { href: "clients.html",            label: "Client Libraries" },
    { href: "backup.html",             label: "Backup &amp; Restore" },
    { href: "systemd.html",            label: "Systemd" },
    { href: "api.html",                label: "API Reference" },
    { href: "architecture.html",       label: "Architecture" },
    { href: "codebase-guide.html",     label: "Codebase Guide" },
    { href: "examples.html",           label: "Examples" }
  ];

  var FOOTER_LINKS = [
    { href: "index.html",      label: "Home" },
    { href: "quickstart.html", label: "Quick Start" },
    { href: "queries.html",    label: "Queries" },
    { href: "configuration.html", label: "Configuration" },
    { href: "api.html",        label: "API Reference" },
    { href: "examples.html",   label: "Examples" },
    { href: "/dashboard",      label: "Live Dashboard" }
  ];

  var HEADER_HTML =
    '<a class="brand" href="index.html" aria-label="sqtseries home">' +
      '<span class="brand-mark" aria-hidden="true">S</span>' +
      '<span class="brand-name">sqtseries</span>' +
    '</a>' +
    '<span class="version-badge">' + VERSION + '</span>' +
    '<span class="spacer"></span>' +
    '<button id="theme-toggle" type="button" aria-label="Toggle dark mode">' +
      '<svg class="icon-light" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>' +
      '<svg class="icon-dark" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>' +
    '</button>';

  function getInitialTheme() {
    try {
      var saved = window.localStorage.getItem(STORAGE_KEY);
      if (saved === "dark" || saved === "light") return saved;
    } catch (e) { /* storage unavailable */ }
    if (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) {
      return "dark";
    }
    return "light";
  }

  function applyTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    try {
      window.localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) { /* ignore */ }
  }

  function slugify(text) {
    return text.toLowerCase().replace(/[^\w\s-]/g, "").trim().replace(/\s+/g, "-");
  }

  function onReady(fn) {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", fn);
    } else {
      fn();
    }
  }

  /* Detect which page we're on from nav a.here (set in the HTML) or URL */
  function detectCurrentHref() {
    var here = document.querySelector('nav[aria-label="Main"] a.here');
    if (here) return here.getAttribute("href");
    var path = window.location.pathname;
    return path.split("/").pop() || "index.html";
  }

  function injectHeader() {
    var header = document.querySelector("header");
    if (header) header.innerHTML = HEADER_HTML;
  }

  function injectNav() {
    var existing = document.querySelector('nav[aria-label="Main"]');
    if (!existing) return;
    var current = detectCurrentHref();
    var html = "";
    NAV_ITEMS.forEach(function (item) {
      var isCurrent = item.href === current;
      var cls = isCurrent ? ' class="here"' : "";
      var cur = isCurrent ? ' aria-current="page"' : "";
      html += "<li><a" + cls + cur + ' href="' + item.href + '">' + item.label + "</a></li>";
    });
    html += '<li><a href="../api-docs" target="_blank" rel="noopener">API Specs</a></li>';
    existing.innerHTML = '<ul id="main-nav-list">' + html + "</ul>";
  }

  function injectNavToggle() {
    var header = document.querySelector("header");
    var nav = document.querySelector('nav[aria-label="Main"]');
    if (!header || !nav) return;
    if (document.getElementById("nav-toggle")) return;
    var btn = document.createElement("button");
    btn.id = "nav-toggle";
    btn.type = "button";
    btn.setAttribute("aria-expanded", "false");
    btn.setAttribute("aria-controls", "main-nav-list");
    btn.setAttribute("aria-label", "Open menu");
    btn.innerHTML =
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" ' +
      'stroke-width="2" stroke-linecap="round" aria-hidden="true">' +
      "<path d='M4 7h16M4 12h16M4 17h16'/></svg>";
    header.insertBefore(btn, header.firstChild);
    document.documentElement.classList.add("js", "nav-ready");

    var list = document.getElementById("main-nav-list");
    function isMobile() {
      return window.matchMedia &&
        window.matchMedia("(max-width: 820px)").matches;
    }
    function refreshInert() {
      if (!list) return;
      var open = document.documentElement.classList.contains("nav-open");
      if (!isMobile() || open) {
        list.removeAttribute("inert");
      } else {
        list.setAttribute("inert", "");
      }
    }
    function setOpen(open) {
      btn.setAttribute("aria-expanded", open ? "true" : "false");
      btn.setAttribute("aria-label", open ? "Close menu" : "Open menu");
      document.documentElement.classList.toggle("nav-open", open);
      refreshInert();
    }
    function syncForWidth() {
      if (!isMobile()) setOpen(false);
      else refreshInert();
    }
    btn.addEventListener("click", function () {
      setOpen(btn.getAttribute("aria-expanded") !== "true");
      if (btn.getAttribute("aria-expanded") === "true" && list) {
        var first = list.querySelector("a");
        if (first) first.focus();
      }
    });
    document.addEventListener("keydown", function (ev) {
      if (ev.key === "Escape" &&
          document.documentElement.classList.contains("nav-open")) {
        setOpen(false);
        btn.focus();
      }
    });
    nav.addEventListener("click", function (ev) {
      if (ev.target && ev.target.tagName === "A") setOpen(false);
    });
    var resizeT = null;
    window.addEventListener("resize", function () {
      if (resizeT) clearTimeout(resizeT);
      resizeT = setTimeout(syncForWidth, 120);
    });
    syncForWidth();
  }

  function injectFooterLinks() {
    var container = document.querySelector(".footer-links");
    if (!container) return;
    var html = "";
    FOOTER_LINKS.forEach(function (item) {
      html += '<a href="' + item.href + '">' + item.label + '</a>';
    });
    html += '<a href="../api-docs" target="_blank" rel="noopener">API Specs</a>';
    container.innerHTML = html;
  }

  onReady(function () {
    applyTheme(getInitialTheme());

    injectHeader();
    injectNav();
    injectNavToggle();
    injectFooterLinks();

    var toggle = document.getElementById("theme-toggle");
    if (toggle) {
      toggle.addEventListener("click", function () {
        var cur = document.documentElement.getAttribute("data-theme") || "light";
        applyTheme(cur === "dark" ? "light" : "dark");
      });
    }

    var main = document.querySelector("main");
    var currentHref = detectCurrentHref();
    var navItem = NAV_ITEMS.find(function (n) { return n.href === currentHref; });
    if (main && navItem) {
      var crumbs = document.createElement("nav");
      crumbs.className = "crumbs";
      crumbs.setAttribute("aria-label", "Breadcrumb");
      var home = document.createElement("a");
      home.href = "index.html";
      home.textContent = "Docs";
      var sep = document.createElement("span");
      sep.className = "sep";
      sep.textContent = "/";
      var here = document.createElement("span");
      here.textContent = navItem.label.replace(/&amp;/g, "&");
      crumbs.appendChild(home);
      crumbs.appendChild(sep);
      crumbs.appendChild(here);
      main.insertBefore(crumbs, main.firstChild);
    }

    var content = document.querySelector(".docs-content");
    var tocList = document.getElementById("toc-list");
    if (content && tocList) {
      var headings = content.querySelectorAll("h2, h3");
      var seen = {};
      headings.forEach(function (h, i) {
        if (h.closest(".toc")) return;
        var id = h.id || slugify(h.textContent) || "section-" + i;
        if (seen[id]) { id = id + "-" + i; }
        seen[id] = true;
        h.id = id;
        var li = document.createElement("li");
        var a = document.createElement("a");
        a.href = "#" + id;
        a.textContent = h.textContent;
        if (h.tagName === "H3") a.className = "level-3";
        li.appendChild(a);
        tocList.appendChild(li);
      });
      /* scroll-spy */
      var links = tocList.querySelectorAll("a");
      if (links.length && "IntersectionObserver" in window) {
        var spy = new IntersectionObserver(function (entries) {
          entries.forEach(function (entry) {
            if (!entry.isIntersecting) return;
            links.forEach(function (l) {
              l.classList.toggle("active", l.getAttribute("href") === "#" + entry.target.id);
            });
          });
        }, { rootMargin: "-80px 0px -70% 0px" });
        headings.forEach(function (h) { spy.observe(h); });
      }
    }

    document.querySelectorAll("pre").forEach(function (pre) {
      var btn = document.createElement("button");
      btn.className = "copy-btn";
      btn.type = "button";
      btn.textContent = "Copy";
      btn.setAttribute("aria-label", "Copy code to clipboard");
      btn.addEventListener("click", function () {
        var text = pre.innerText || pre.textContent || "";
        var done = function () {
          btn.textContent = "Copied";
          btn.classList.add("copied");
          setTimeout(function () {
            btn.textContent = "Copy";
            btn.classList.remove("copied");
          }, 1500);
        };
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(done).catch(done);
        } else {
          done();
        }
      });
      pre.appendChild(btn);
    });

    var footer = document.querySelector("footer");
    if (footer) {
      var order = NAV_ITEMS.map(function (n) { return n.href; });
      var idx = order.indexOf(currentHref);
      var pag = document.createElement("nav");
      pag.className = "pagination";
      pag.setAttribute("aria-label", "Page navigation");
      if (idx > 0) {
        var prevItem = NAV_ITEMS[idx - 1];
        var prev = document.createElement("a");
        prev.href = prevItem.href;
        prev.innerHTML = '<span class="dir">Previous</span><span class="page">' +
                         prevItem.label.replace(/&amp;/g, "&") + "</span>";
        pag.appendChild(prev);
      } else {
        var ph = document.createElement("span");
        ph.hidden = true;
        pag.appendChild(ph);
      }
      if (idx < order.length - 1) {
        var nextItem = NAV_ITEMS[idx + 1];
        var next = document.createElement("a");
        next.href = nextItem.href;
        next.className = "next";
        next.innerHTML = '<span class="dir">Next</span><span class="page">' +
                         nextItem.label.replace(/&amp;/g, "&") + "</span>";
        pag.appendChild(next);
      }
      footer.insertBefore(pag, footer.firstChild);
    }
  });
})();
