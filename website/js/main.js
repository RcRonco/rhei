(() => {
  // Scroll reveal
  const reveals = document.querySelectorAll('.reveal');
  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting) {
          entry.target.classList.add('visible');
          observer.unobserve(entry.target);
        }
      }
    },
    { threshold: 0.08, rootMargin: '0px 0px -40px 0px' }
  );

  for (const el of reveals) {
    observer.observe(el);
  }

  // Copy to clipboard
  const copyBtns = document.querySelectorAll('[data-copy]');
  for (const btn of copyBtns) {
    btn.addEventListener('click', () => {
      const text = btn.getAttribute('data-copy');
      navigator.clipboard.writeText(text).then(() => {
        const hint = btn.querySelector('.copy-hint');
        if (hint) {
          hint.textContent = 'copied';
          setTimeout(() => { hint.textContent = 'copy'; }, 2000);
        }
      });
    });
  }
})();
