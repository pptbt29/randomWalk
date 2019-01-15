package au.csiro.data61.randomwalk.tool

object PhoneNumberFilter {
  def regexCheckAndSlicePhoneNumber(phoneNumber: String): String = {
    if (phoneNumber.matches("^(|0*86)(1[34578]\\d{9})$"))
      phoneNumber.takeRight(11)
    else ""
  }
}
