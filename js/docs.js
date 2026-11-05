(() => {
  const menu = document.getElementById("doc-menu");
  if (!menu || !window.matchMedia) return;

  const phone = window.matchMedia("(max-width: 640px)");
  const setMenu = () => {
    menu.open = !phone.matches;
  };

  setMenu();
  phone.addEventListener("change", setMenu);
})();
