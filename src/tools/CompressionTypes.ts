export const enum CompressionTypesEnum {
  NONE = 0,
  GZIP = 1,
  SNAPPY = 2,
}
const TYPES: CompressionTypesEnum[] = [0, 1, 2];

export class CompressionTypesClass {

  public readonly NONE: number = CompressionTypesEnum.NONE;
  public readonly GZIP: number = CompressionTypesEnum.GZIP;
  public readonly SNAPPY: number = CompressionTypesEnum.SNAPPY;

  public isValid(type: number): boolean {
    if (typeof type !== "number") {
      return false;
    }

    return TYPES.indexOf(type) !== -1;
  }
}

export const CompressionTypes = new CompressionTypesClass();
