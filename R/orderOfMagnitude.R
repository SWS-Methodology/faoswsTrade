orderOfMagnitude <- function(x, y, mode = 'ceiling') {
  if (mode == 'ceiling') {
    x/(10^(ceiling(log10(x/y))))
  } else if (mode == 'round') {
    x/(10^(round(log10(x/y))))
  } else {
    stop('No mode')
  }
}
