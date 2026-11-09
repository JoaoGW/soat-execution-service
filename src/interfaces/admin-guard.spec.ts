import { ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { sign } from 'jsonwebtoken';
import { AdminGuard } from './admin-guard';

describe('AdminGuard', () => {
  const original = process.env; const secret = 'a'.repeat(40);
  const context = (authorization?: string) => ({ switchToHttp: () => ({ getRequest: () => ({ headers: { authorization } }) }) }) as unknown as ExecutionContext;
  beforeEach(() => { process.env = { ...original, JWT_SECRET: secret, JWT_CLIENT_SECRET: 'b'.repeat(40) }; });
  afterEach(() => { process.env = original; });
  it('aceita somente token administrativo assinado, com validade', () => {
    const guard = new AdminGuard();
    expect(guard.canActivate(context(`Bearer ${sign({ sub: 'operator', role: 'admin' }, secret, { expiresIn: '1h' })}`))).toBe(true);
    for (const token of [sign({ sub: 'operator', role: 'cliente' }, secret, { expiresIn: '1h' }), sign({ sub: 'operator', role: 'admin' }, secret), sign({ sub: 'operator', role: 'admin' }, secret, { expiresIn: -1 }), sign({ sub: 'operator', role: 'admin' }, 'other', { expiresIn: '1h' })]) {
      expect(() => guard.canActivate(context(`Bearer ${token}`))).toThrow(UnauthorizedException);
    }
    expect(() => guard.canActivate(context())).toThrow(UnauthorizedException);
    process.env.JWT_CLIENT_SECRET = secret;
    expect(() => guard.canActivate(context(`Bearer ${sign({ sub: 'operator', role: 'admin' }, secret, { expiresIn: '1h' })}`))).toThrow(UnauthorizedException);
  });
});
