export const isEmptyObject = (o: any) => {
  return (
    typeof o === 'object' &&
    o !== null &&
    (Object.keys(o).length === 0 ||
      Object.values(o).every(
        i => typeof i === 'undefined' || i === null || i === ''
      ))
  )
}
