export const isEmptyObject = (o: any) => {
  return typeof o === 'object' && o !== null && Object.keys(o).length === 0
}
