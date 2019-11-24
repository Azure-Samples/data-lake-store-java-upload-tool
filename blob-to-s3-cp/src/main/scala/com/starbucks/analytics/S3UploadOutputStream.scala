package com.starbucks.analytics

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}


/**
  * Created by depatel on 8/24/17.
  */
class S3UploadOutputStream extends ByteArrayOutputStream{

  def toInputStream: ByteArrayInputStream ={
    new ByteArrayInputStream(buf, 0, count)
  }
}
