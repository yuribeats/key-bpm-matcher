import { next } from '@vercel/edge';

export default function middleware() {
  return next({
    headers: {
      'x-proxy-secret': process.env.PROXY_SECRET,
    },
  });
}

export const config = {
  matcher: '/api/:path*',
};
