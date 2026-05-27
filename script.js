const nav = document.getElementById('nav');
const navLinks = document.getElementById('navLinks');
const menuBtn = document.getElementById('menuBtn');
const progress = document.getElementById('scrollProgress');
const backTop = document.getElementById('backTop');
const cursorGlow = document.getElementById('cursorGlow');

menuBtn?.addEventListener('click', () => {
  const isOpen = navLinks.classList.toggle('open');
  menuBtn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
});

document.querySelectorAll('.nav-links a').forEach(link => {
  link.addEventListener('click', () => {
    navLinks.classList.remove('open');
    menuBtn.setAttribute('aria-expanded', 'false');
  });
});

const updateScrollUI = () => {
  const scrollTop = window.scrollY;
  const docHeight = document.documentElement.scrollHeight - window.innerHeight;
  const width = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;
  progress.style.width = `${width}%`;
  nav.classList.toggle('scrolled', scrollTop > 20);
  backTop.classList.toggle('show', scrollTop > 500);
};
window.addEventListener('scroll', updateScrollUI);
updateScrollUI();

backTop?.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));

if (window.matchMedia('(pointer:fine)').matches && cursorGlow) {
  window.addEventListener('mousemove', (event) => {
    cursorGlow.style.left = `${event.clientX}px`;
    cursorGlow.style.top = `${event.clientY}px`;
  });
}

const reveals = document.querySelectorAll('.reveal');
const revealObserver = new IntersectionObserver((entries) => {
  entries.forEach((entry) => {
    if (!entry.isIntersecting) return;
    entry.target.classList.add('show');
    revealObserver.unobserve(entry.target);
  });
}, { threshold: 0.12 });
reveals.forEach((element) => revealObserver.observe(element));

const counters = document.querySelectorAll('[data-count]');
const countObserver = new IntersectionObserver((entries) => {
  entries.forEach((entry) => {
    if (!entry.isIntersecting) return;
    const target = entry.target;
    const end = Number(target.dataset.count);
    let current = 0;
    const step = Math.max(1, Math.ceil(end / 36));
    const timer = setInterval(() => {
      current += step;
      if (current >= end) {
        target.textContent = `${end}+`;
        clearInterval(timer);
      } else {
        target.textContent = current;
      }
    }, 28);
    countObserver.unobserve(target);
  });
}, { threshold: 0.7 });
counters.forEach((counter) => countObserver.observe(counter));

const sections = document.querySelectorAll('main section[id]');
const navItems = document.querySelectorAll('.nav-links a[href^="#"]');
const activeObserver = new IntersectionObserver((entries) => {
  entries.forEach((entry) => {
    if (!entry.isIntersecting) return;
    const id = entry.target.getAttribute('id');
    navItems.forEach((item) => {
      item.classList.toggle('active', item.getAttribute('href') === `#${id}`);
    });
  });
}, { rootMargin: '-45% 0px -45% 0px', threshold: 0 });
sections.forEach((section) => activeObserver.observe(section));

if (window.matchMedia('(pointer:fine)').matches) {
  document.querySelectorAll('.tilt-card').forEach((card) => {
    card.addEventListener('mousemove', (event) => {
      const rect = card.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const rotateX = ((y / rect.height) - 0.5) * -6;
      const rotateY = ((x / rect.width) - 0.5) * 6;
      card.style.transform = `perspective(900px) rotateX(${rotateX}deg) rotateY(${rotateY}deg) translateY(-4px)`;
    });
    card.addEventListener('mouseleave', () => {
      card.style.transform = '';
    });
  });
}
