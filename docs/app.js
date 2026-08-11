/* sqtseries docs — progressive enhancement.
   Theme toggle, on-this-page TOC, breadcrumbs, code copy buttons,
   prev/next pagination. All features degrade gracefully. */

(function () {
  "use strict";

  var STORAGE_KEY = "sqtseries-theme";

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

  onReady(function () {
    applyTheme(getInitialTheme());

    /* ---- theme toggle ---- */
    var toggle = document.getElementById("theme-toggle");
    if (toggle) {
      toggle.addEventListener("click", function () {
        var cur = document.documentElement.getAttribute("data-theme") || "light";
        applyTheme(cur === "dark" ? "light" : "dark");
      });
    }

    /* ---- breadcrumb ---- */
    var main = document.querySelector("main");
    var current = document.querySelector('nav a.here');
    if (main && current) {
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
      here.textContent = current.textContent;
      crumbs.appendChild(home);
      crumbs.appendChild(sep);
      crumbs.appendChild(here);
      main.insertBefore(crumbs, main.firstChild);
    }

    /* ---- on-this-page TOC ---- */
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
      /* scroll-spy: highlight the heading currently in view */
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

    /* ---- code copy buttons ---- */
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

    /* ---- prev / next pagination ---- */
    var footer = document.querySelector("footer");
    if (footer) {
      var order = ["index.html", "quickstart.html", "configuration.html",
                   "ingestion.html", "queries.html", "streaming.html",
                   "camera.html", "clients.html", "backup.html", "systemd.html",
                   "api.html", "architecture.html", "codebase-guide.html", "examples.html"];
      var here = document.querySelector('nav a.here');
      if (here) {
        var cur = here.getAttribute("href");
        var idx = order.indexOf(cur);
        var pag = document.createElement("nav");
        pag.className = "pagination";
        pag.setAttribute("aria-label", "Page navigation");
        if (idx > 0) {
          var prev = document.createElement("a");
          prev.href = order[idx - 1];
          prev.innerHTML = '<span class="dir">Previous</span><span class="page">' +
                           document.querySelector('nav a[href="' + order[idx - 1] + '"]').textContent +
                           "</span>";
          pag.appendChild(prev);
        } else {
          var ph = document.createElement("span");
          ph.hidden = true;
          pag.appendChild(ph);
        }
        if (idx < order.length - 1) {
          var next = document.createElement("a");
          next.href = order[idx + 1];
          next.className = "next";
          next.innerHTML = '<span class="dir">Next</span><span class="page">' +
                           document.querySelector('nav a[href="' + order[idx + 1] + '"]').textContent +
                           "</span>";
          pag.appendChild(next);
        }
        footer.insertBefore(pag, footer.firstChild);
      }
    }
  });
})();
