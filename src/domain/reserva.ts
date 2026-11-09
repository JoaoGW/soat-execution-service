export type ReservationStatus = 'RESERVED' | 'CONSUMED' | 'RELEASED';
export type Stock = { sku: string; balance: number; reserved: number };
export type Line = { sku: string; quantity: number };
export type Reservation = { id: string; status: ReservationStatus; items: Line[] };

// These functions calculate a transition only. The caller must persist stock,
// reservation, inbox and outbox in one Cosmos batch, with ETag preconditions.
export function reserve(id: string, lines: Line[], stock: Stock[]): { reservation: Reservation; stock: Stock[] } {
  if (!id) throw new Error('Reserva inválida');
  const quantities = new Map<string, number>();
  for (const line of lines) {
    if (!line.sku || !Number.isSafeInteger(line.quantity) || line.quantity <= 0) throw new Error('Item inválido');
    const total = (quantities.get(line.sku) ?? 0) + line.quantity;
    if (!Number.isSafeInteger(total)) throw new Error('Quantidade inválida');
    quantities.set(line.sku, total);
  }
  if (quantities.size > 20) throw new Error('Máximo de 20 SKUs');
  const stockBySku = new Map<string, Stock>();
  for (const item of stock) {
    if (!item.sku || stockBySku.has(item.sku) || !Number.isSafeInteger(item.balance) || !Number.isSafeInteger(item.reserved) || item.balance < 0 || item.reserved < 0 || item.reserved > item.balance) throw new Error('Estoque inválido');
    stockBySku.set(item.sku, item);
  }
  for (const [sku, quantity] of quantities) {
    const item = stockBySku.get(sku);
    if (!item || item.balance - item.reserved < quantity) throw new Error('Estoque insuficiente');
  }
  return {
    reservation: { id, status: 'RESERVED', items: [...quantities].map(([sku, quantity]) => ({ sku, quantity })) },
    stock: stock.map((item) => ({ ...item, reserved: item.reserved + (quantities.get(item.sku) ?? 0) })),
  };
}

export function settle(reservation: Reservation, stock: Stock[], action: 'CONSUME' | 'RELEASE'): { reservation: Reservation; stock: Stock[] } {
  const target = action === 'CONSUME' ? 'CONSUMED' : 'RELEASED';
  if (reservation.status === target) return { reservation, stock };
  if (reservation.status !== 'RESERVED') throw new Error('Reserva já finalizada');
  const quantities = new Map(reservation.items.map((item) => [item.sku, item.quantity]));
  if (quantities.size !== reservation.items.length || stock.some((item, i) => stock.findIndex((other) => other.sku === item.sku) !== i)) throw new Error('Estado de estoque inválido');
  for (const [sku, quantity] of quantities) {
    const item = stock.find((entry) => entry.sku === sku);
    if (!item || !Number.isSafeInteger(quantity) || quantity <= 0 || !Number.isSafeInteger(item.reserved) || !Number.isSafeInteger(item.balance) || item.reserved < quantity || item.balance < item.reserved) throw new Error('Estado de estoque inválido');
  }
  return {
    reservation: { ...reservation, status: target },
    stock: stock.map((item) => {
      const quantity = quantities.get(item.sku) ?? 0;
      return { ...item, reserved: item.reserved - quantity, balance: item.balance - (action === 'CONSUME' ? quantity : 0) };
    }),
  };
}
