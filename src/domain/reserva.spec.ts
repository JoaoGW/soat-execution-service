import { reserve, settle } from './reserva';

describe('reserva de estoque', () => {
  const stock = [{ sku: 'peca', balance: 5, reserved: 1 }];
  it('agrega SKUs e reserva sem reduzir saldo físico', () => {
    const result = reserve('os-1', [{ sku: 'peca', quantity: 1 }, { sku: 'peca', quantity: 2 }], stock);
    expect(result.reservation.items).toEqual([{ sku: 'peca', quantity: 3 }]);
    expect(result.stock).toEqual([{ sku: 'peca', balance: 5, reserved: 4 }]);
    expect(stock[0].reserved).toBe(1);
  });
  it('rejeita insuficiência antes de modificar qualquer saldo', () => {
    expect(() => reserve('os-1', [{ sku: 'peca', quantity: 5 }], stock)).toThrow('Estoque insuficiente');
    expect(stock[0].reserved).toBe(1);
  });
  it('rejeita uma segunda reserva quando a primeira esgotou o disponível', () => {
    const first = reserve('os-1', [{ sku: 'peca', quantity: 4 }], stock);
    expect(() => reserve('os-2', [{ sku: 'peca', quantity: 1 }], first.stock)).toThrow('Estoque insuficiente');
  });
  it('consome uma vez e impede posterior liberação', () => {
    const first = reserve('os-1', [{ sku: 'peca', quantity: 2 }], stock);
    const consumed = settle(first.reservation, first.stock, 'CONSUME');
    expect(consumed.stock).toEqual([{ sku: 'peca', balance: 3, reserved: 1 }]);
    expect(settle(consumed.reservation, consumed.stock, 'CONSUME')).toEqual(consumed);
    expect(() => settle(consumed.reservation, consumed.stock, 'RELEASE')).toThrow('já finalizada');
  });
  it('libera sem alterar o saldo físico e repete de forma idempotente', () => {
    const first = reserve('os-1', [{ sku: 'peca', quantity: 2 }], stock);
    const released = settle(first.reservation, first.stock, 'RELEASE');
    expect(released.stock).toEqual(stock);
    expect(settle(released.reservation, released.stock, 'RELEASE')).toEqual(released);
  });
  it('limita SKUs e rejeita quantidades inválidas', () => {
    expect(() => reserve('os-1', Array.from({ length: 21 }, (_, i) => ({ sku: `${i}`, quantity: 1 })), [])).toThrow('20 SKUs');
    expect(() => reserve('os-1', [{ sku: 'peca', quantity: 0 }], stock)).toThrow('Item inválido');
  });
});
