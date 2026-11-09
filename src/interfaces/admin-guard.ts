import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import { verify } from 'jsonwebtoken';

@Injectable()
export class AdminGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<{ headers: Record<string, string | string[] | undefined>; adminId?: string }>();
    const header = request.headers.authorization;
    const secret = process.env.JWT_SECRET;
    if (!secret || Buffer.byteLength(secret, 'utf8') < 32 || secret === process.env.JWT_CLIENT_SECRET || typeof header !== 'string' || !header.startsWith('Bearer ')) throw new UnauthorizedException();
    try {
      const claims = verify(header.slice(7), secret, { algorithms: ['HS256'] });
      if (typeof claims === 'string' || claims.role !== 'admin' || typeof claims.sub !== 'string' || !claims.sub.trim() || typeof claims.exp !== 'number' || !Number.isFinite(claims.exp)) throw new Error('Claims inválidas');
      request.adminId = claims.sub;
      return true;
    } catch { throw new UnauthorizedException(); }
  }
}
