```scala

object Morse {
    /**
    * decode any morseEncoded string with all morse supported chars (A-Z and 0-9)
    * @param Morse
    * @return 
    */

    val encodingForChar = Map(
    'A' ->".-", 'B' -> "-...", 'C' -> "-.-.", 'D' -> "-..", 'E' -> ".",
    'F' -> "..-.", 'G' -> "--.",  'H' -> "....", 'I' -> "..", 'J' -> ".---",
    'K' -> "-.-", 'L' -> ".-..", 'M' -> "--", 'N' -> "-.", 'O' -> "---",
    'P' -> ".--.", 'Q' -> "--.-", 'R' -> ".-.", 'S' -> "...", 'T' -> "-",
    'U' -> "..-", 'V' -> "...-", 'W' -> ".--", 'X' -> "-..-", 'Y' -> "-.--",
    'Z' -> "--..", '0' -> "-----", '1' -> ".----", '2' -> "..---", '3' -> "...--",
    '4' -> "....-", '5' -> ".....", '6' -> "-....", '7' -> "--...", '8' -> "---..",
    '9' -> "----."
    )
    //def morseEncode(s: String) = s.map(encodingForChar).mkString(" ")
    //morseEncode("SOS")
    def morseDecode(s: String) = {
        /**
        * get the key from the value in the encodingForChar Map
        * 
        * @param s: morse code value to be decoded
        * @return : decoded value
        */
        printf(encodingForChar.filter(_._2 == s).map(_._1).mkString(" "))
    }

    def main(args: Array[String]) = {
        args.map(morseDecode)
        println(" ")
    }
}
```