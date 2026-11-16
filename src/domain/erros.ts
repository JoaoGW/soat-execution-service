/**
 * Erro de regra de negócio conhecido. A camada HTTP traduz `INVALID_INPUT` em 400,
 * `NOT_FOUND` em 404, `CONFLICT` em 409 e `UNPROCESSABLE` em 422; qualquer outro erro continua 500.
 */
export type BusinessErrorKind = 'CONFLICT' | 'INVALID_INPUT' | 'NOT_FOUND' | 'UNPROCESSABLE';
export class BusinessError extends Error {
  constructor(message: string, readonly kind: BusinessErrorKind) { super(message); this.name = 'BusinessError'; }
}
export const conflict = (message: string): BusinessError => new BusinessError(message, 'CONFLICT');
export const invalidInput = (message: string): BusinessError => new BusinessError(message, 'INVALID_INPUT');
export const notFound = (message: string): BusinessError => new BusinessError(message, 'NOT_FOUND');
export const unprocessable = (message: string): BusinessError => new BusinessError(message, 'UNPROCESSABLE');
