const UPSTREAM = 'http://204.168.175.190:3000';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ error: 'POST required' });
    return;
  }

  let body = '';
  for await (const chunk of req) body += chunk;

  const upstream = await fetch(`${UPSTREAM}/api/register`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-proxy-secret': process.env.PROXY_SECRET,
    },
    body,
  });
  const data = await upstream.text();
  res.setHeader('Content-Type', 'application/json');
  res.status(upstream.status).send(data);
}
