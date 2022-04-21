package edu.ucr.dblab.tests

object PairingFunctions {

  //****************************************
  //* http://szudzik.com/ElegantPairing.pdf
  //****************************************
  def anElegantPairingFunction(a: Int, b: Int): Long = {
    def op(x: Int): Long = if( x >= 0 ){ 2 * x.toLong} else { -2 * x.toLong - 1 }
    val A = op(a)
    val B = op(b)
    val C = { if(A >= B){ A * A + A + B } else { A + B * B } } / 2

    if( a < 0 && b < 0 || a >= 0 && b >= 0 ) C else -C - 1
  }
}
