package au.csiro.data61.randomwalk.tool

class Main {
  def main(args: Array[String]): Unit = {
    PhoneNumberPairTool.setPhoneNumber2vec("syang/output/number2vec")
    PhoneNumberPairTool.getTopTenSimilarity(args(0))
  }
}
