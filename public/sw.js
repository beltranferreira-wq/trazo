// Service Worker desactivado - limpia todo el caché anterior
self.addEventListener('install', e => { self.skipWaiting(); });
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(keys.map(k => caches.delete(k))))
    .then(() => self.clients.claim())
    .then(() => {
      // Notificar a todos los clientes que recarguen
      self.clients.matchAll().then(clients => {
        clients.forEach(client => client.postMessage({ type: 'CACHE_CLEARED' }));
      });
    })
  );
});
// No interceptar ningún fetch - dejar pasar todo al servidor
