const UPSTREAM = 'http://204.168.175.190:3000';

export default async function handler(req, res) {
  const qs = new URL(req.url, `http://${req.headers.host}`).search;
  const upstream = await fetch(`${UPSTREAM}/api/search${qs}`, {
    headers: {
      'x-proxy-secret': process.env.PROXY_SECRET,
    },
  });
  const data = await upstream.text();
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');
  res.status(upstream.status).send(data);
}
