import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'node:net';
import { sign } from 'jsonwebtoken';
import { AppModule } from '../app.module';

describe('autorização HTTP das ações de Execution', () => {
  let app: INestApplication; let url: string; const original = process.env; const secret = 'secret-admin-test-'.repeat(3);
  beforeAll(async () => {
    process.env = { ...original, JWT_SECRET: secret, JWT_CLIENT_SECRET: 'client-secret-test-'.repeat(3) };
    delete process.env.ENABLE_EXECUTION_COMMANDS; delete process.env.ENABLE_EXECUTION_OUTBOX; delete process.env.COSMOS_ENDPOINT;
    app = await NestFactory.create(AppModule, { logger: false });
    await app.listen(0, '127.0.0.1');
    url = `http://127.0.0.1:${(app.getHttpServer().address() as AddressInfo).port}`;
  });
  afterAll(async () => { await app?.close(); process.env = original; });
  it('nega token de cliente e aceita admin antes de verificar configuração do banco', async () => {
    const send = async (role: string) => fetch(`${url}/execucoes/os1/iniciar`, { method: 'POST', headers: { 'content-type': 'application/json', 'idempotency-key': 'op1', authorization: `Bearer ${sign({ sub: 'user', role }, secret, { expiresIn: '1h' })}` }, body: JSON.stringify({ sagaId: 's1', filialId: 'f1', executionId: 'e1' }) });
    expect((await send('cliente')).status).toBe(401);
    expect((await send('admin')).status).toBe(503);
    expect((await fetch(`${url}/catalogo/f1/PART/p1`)).status).toBe(401);
  });
});
