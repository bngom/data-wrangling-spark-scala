# data-wrangling-spark-scala

## Challenge #1

[parse a CSV file](./PARSE-CSV.md)

## Challenge #2

[decode a morse message](./MORSE-DECODE.md)

## Challenge #3

Create a scala file containing a function
* def createReport(gzPath: String, outputPath: String): Unit // signature of the function
* the gzPath is the file access.log.gz 
* the function find all the dates having too big number of connection (> 20000)
* for each date

    - compute the list of number of access by URI for each URI
    - compute the list of number of access per IP address for each IP address

* generate at outputPath a report in json format with one json report line per date


### Usage

```
git clone https://github.com/bngom/data-wrangling-spark-scala.git
cd data-wrangling-spark-scala
```

```
get the logs curl -o access.log.gz -L https://github.com/jlcanela/spark-hands-on/raw/master/almhuette-raith.log/access.log.gz
```

sbt configuration

```
// build.sbt

ThisBuild / scalaVersion := "2.12.7"
ThisBuild / organization := "com.bn"


val scalaTest = "org.scalatest" %% "scalatest" % "3.2.2"

lazy val app = (project in file("."))
  .settings(
    name := "App",
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1",
    libraryDependencies += scalaTest % "test",
  )
```

From the root directory of the project execute.

```
sbt run
```

### Test

From the root directory of the project execute.

```
sbt test
```

### Packaging

From the root directory of the project create a file `assembly.sbt` in the forlder `project`(/project/assembly.sbt).

Edit the file with the following code.

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
```

```scala
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "LogReporting-assembly-1.0.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
```

Generate the jar file int the folder `target/scala-2.12`.

```scala
sbt assembly
```

From the root directory of the project run using spark-submit.

```
spark-submit target/scala-2.12/LogReporting-assembly-1.0.jar
```

Check the report in the `output` folder.

```json
{"date":"2018-05-28","IP":[{"ip":"103.70.216.15","ipCount":2},{"ip":"54.36.148.126","ipCount":1},{"ip":"188.23.238.42","ipCount":11},{"ip":"66.249.66.145","ipCount":4},{"ip":"207.46.13.136","ipCount":1},{"ip":"67.174.217.62","ipCount":3},{"ip":"197.210.52.126","ipCount":1},{"ip":"178.132.78.55","ipCount":2},{"ip":"159.89.131.181","ipCount":1},{"ip":"193.106.121.228","ipCount":31},{"ip":"66.249.66.143","ipCount":2},{"ip":"88.202.188.67","ipCount":4543},{"ip":"5.113.35.73","ipCount":50224},{"ip":"40.77.189.101","ipCount":15},{"ip":"80.110.112.181","ipCount":93},{"ip":"62.82.48.2","ipCount":2},{"ip":"157.32.40.151","ipCount":1},{"ip":"52.59.114.154","ipCount":1},{"ip":"66.102.6.148","ipCount":1},{"ip":"77.72.84.18","ipCount":58},{"ip":"62.210.146.46","ipCount":3},{"ip":"62.107.233.208","ipCount":1},{"ip":"216.244.66.230","ipCount":5},{"ip":"178.137.92.135","ipCount":5},{"ip":"193.110.182.10","ipCount":87},{"ip":"138.201.30.66","ipCount":9},{"ip":"69.143.173.65","ipCount":1},{"ip":"178.137.86.85","ipCount":1},{"ip":"157.55.39.32","ipCount":2},{"ip":"40.77.167.40","ipCount":1},{"ip":"176.212.185.100","ipCount":2},{"ip":"195.201.188.26","ipCount":5},{"ip":"104.131.147.112","ipCount":1},{"ip":"66.102.6.149","ipCount":1},{"ip":"207.46.13.150","ipCount":2},{"ip":"95.90.240.174","ipCount":121},{"ip":"106.51.72.22","ipCount":1},{"ip":"151.80.39.80","ipCount":1},{"ip":"42.236.103.106","ipCount":1},{"ip":"18.236.214.55","ipCount":1},{"ip":"159.224.139.35","ipCount":10},{"ip":"188.165.200.217","ipCount":1},{"ip":"180.235.118.134","ipCount":2},{"ip":"157.55.39.96","ipCount":1},{"ip":"95.108.213.19","ipCount":6},{"ip":"157.55.39.99","ipCount":1},{"ip":"178.137.88.8","ipCount":2},{"ip":"157.50.93.149","ipCount":4},{"ip":"162.220.51.40","ipCount":2},{"ip":"172.106.148.128","ipCount":1},{"ip":"66.249.66.141","ipCount":2},{"ip":"5.113.18.208","ipCount":157674},{"ip":"46.229.168.72","ipCount":4},{"ip":"213.225.32.46","ipCount":11},{"ip":"40.77.189.193","ipCount":10},{"ip":"131.107.160.3","ipCount":2},{"ip":"165.225.104.86","ipCount":5},{"ip":"185.45.13.157","ipCount":2},{"ip":"46.30.165.52","ipCount":1},{"ip":"46.185.74.179","ipCount":3},{"ip":"43.245.123.99","ipCount":1},{"ip":"46.229.168.68","ipCount":1},{"ip":"195.170.168.40","ipCount":2},{"ip":"178.255.215.64","ipCount":2},{"ip":"80.123.53.87","ipCount":11},{"ip":"157.55.39.97","ipCount":1},{"ip":"178.132.78.54","ipCount":18},{"ip":"86.52.184.33","ipCount":2}],"URI":[{"uri":"/apache-log/?D=A","uriCount":1},{"uri":"/images/stories/slideshow/almhuette_raith_07.jpg","uriCount":5},{"uri":"/images/stories/slideshow/almhuette_raith_04.jpg","uriCount":5},{"uri":"/index.php?format=feed&type=rss","uriCount":1},{"uri":"/media/system/css/modal.css","uriCount":3},{"uri":"/images/stories/raith/almhuette_raith.jpg","uriCount":5},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_012.jpg","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_l_almhuette_raith_001.jpg","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_001.jpg","uriCount":2},{"uri":"/icons/blank.gif","uriCount":1},{"uri":"/templates/_system/css/general.css","uriCount":38},{"uri":"//components/com_hwdvideoshare/assets/uploads/flash/flash_upload.php","uriCount":2},{"uri":"/components/com_phocagallery/assets/images/icon-folder-medium.gif","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_007.jpg","uriCount":2},{"uri":"/favicon.ico","uriCount":20},{"uri":"/media/system/js/caption.js","uriCount":5},{"uri":"/index.php?option=com_phocagallery&view=category&id=1&Itemid=53","uriCount":4},{"uri":"/index.php?option=com_content&view=article&id=49&itemid=55","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_jaegerzaun_gr.jpg","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_009.jpg","uriCount":2},{"uri":"/templates/jp_hotel/images/module_heading.gif","uriCount":6},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_l_terasse.jpg","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_008.jpg","uriCount":2},{"uri":"/index.php?option=com_phocagallery&view=category&id=4:ferienwohnung2&Itemid=53","uriCount":1},{"uri":"/index.php?format=feed&type=atom","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith.jpg","uriCount":2},{"uri":"/images/phocagallery/Ferienwohnung_2/thumbs/phoca_thumb_m_4_schlafzimmer.jpg","uriCount":1},{"uri":"/components/com_phocagallery/assets/images/icon-up-images.gif","uriCount":3},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/skin/classic/icons/play.png","uriCount":3},{"uri":"/modules/mod_bowslideshow/tmpl/images/image_shadow.png","uriCount":6},{"uri":"/apple-touch-icon.png","uriCount":6},{"uri":"/index.php?option=com_content&view=article&id=49&Itemid=55","uriCount":6},{"uri":"/images/phocagallery/Ferienwohnung_2/thumbs/phoca_thumb_m_6_wc.jpg","uriCount":1},{"uri":"/apache-log/","uriCount":1031},{"uri":"/templates/jp_hotel/css/layout.css","uriCount":6},{"uri":"/wp-login.php","uriCount":2},{"uri":"/components/com_phocagallery/assets/images/icon-view.gif","uriCount":3},{"uri":"/components/com_phocagallery/assets/js/shadowbox/shadowbox.js","uriCount":3},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/skin/classic/skin.css","uriCount":3},{"uri":"/icons/back.gif","uriCount":1},{"uri":"/apache-log/?N=A","uriCount":1},{"uri":"/images/stories/slideshow/almhuette_raith_02.jpg","uriCount":5},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_004.jpg","uriCount":2},{"uri":"/apache-log/access.log","uriCount":211484},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_011.jpg","uriCount":2},{"uri":"/apache-log/?N=D","uriCount":1},{"uri":"/index.php?option=com_content&view=article&id=50&Itemid=56","uriCount":5},{"uri":"/images/stories/raith/garage.jpg","uriCount":5},{"uri":"/images/bg_raith.jpg","uriCount":4},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_aussicht.jpg","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_l_almhuette_raith_005.jpg","uriCount":1},{"uri":"/index.old1","uriCount":1},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/lang/shadowbox-en.js","uriCount":3},{"uri":"/index.php?option=com_phocagallery&view=category&id=1'A=0&Itemid=53","uriCount":1},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/skin/classic/icons/next.png","uriCount":3},{"uri":"/index.php?option=com_phocagallery'A=0&view=category&id=1&Itemid=53","uriCount":1},{"uri":"/templates/jp_hotel/images/logo.jpg","uriCount":6},{"uri":"/apple-touch-icon-precomposed.png","uriCount":6},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/skin/classic/icons/pause.png","uriCount":3},{"uri":"/modules/mod_bowslideshow/tmpl/css/bowslideshow.css","uriCount":6},{"uri":"/templates/jp_hotel/css/template.css","uriCount":6},{"uri":"/images/stories/raith/wohnraum.jpg","uriCount":5},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_garage.jpg","uriCount":2},{"uri":"/images/stories/slideshow/almhuette_raith_01.jpg","uriCount":5},{"uri":"/components/com_phocagallery/assets/images/shadow1.gif","uriCount":3},{"uri":"/icons/text.gif","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_010.jpg","uriCount":2},{"uri":"/apple-touch-icon-120x120-precomposed.png","uriCount":4},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/skin/classic/skin.js","uriCount":3},{"uri":"/media/system/js/mootools.js","uriCount":5},{"uri":"/images/stories/raith/oststeiermark.png","uriCount":5},{"uri":"/images/phocagallery/Ferienwohnung_2/thumbs/phoca_thumb_m_3_wohnkche1.jpg","uriCount":1},{"uri":"/images/phocagallery/Ferienwohnung_2/thumbs/phoca_thumb_m_2_wohnkche.jpg","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_l_zimmer.jpg","uriCount":1},{"uri":"/images/stories/raith/steiermark_herz.png","uriCount":5},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_006.jpg","uriCount":2},{"uri":"/butlerfh.com","uriCount":1},{"uri":"/apache-log//components/com_hwdvideoshare/assets/uploads/flash/flash_upload.php","uriCount":2},{"uri":"/media/system/js/modal.js","uriCount":3},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/player/shadowbox-img.js","uriCount":3},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/skin/classic/icons/previous.png","uriCount":3},{"uri":"/templates/jp_hotel/js/moomenu.js","uriCount":5},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_wohnraum.jpg","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_002.jpg","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_l_almhuette_raith_003.jpg","uriCount":1},{"uri":"/images/stories/slideshow/almhuette_raith_05.jpg","uriCount":5},{"uri":"/images/stories/slideshow/almhuette_raith_03.jpg","uriCount":5},{"uri":"/images/phocagallery/Ferienwohnung_2/thumbs/phoca_thumb_m_1_essecke.jpg","uriCount":1},{"uri":"/administrator/index.php","uriCount":90},{"uri":"/images/stories/raith/wohnung_2_web.jpg","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_l_wohnraum.jpg","uriCount":1},{"uri":"/modules/mod_bowslideshow/tmpl/js/sliderman.1.3.0.js","uriCount":5},{"uri":"/templates/jp_hotel/css/suckerfish.css","uriCount":6},{"uri":"/templates/jp_hotel/css/menu.css","uriCount":6},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/skin/classic/icons/close.png","uriCount":3},{"uri":"/apache-log/error.log","uriCount":2},{"uri":"/apple-touch-icon-152x152-precomposed.png","uriCount":2},{"uri":"/apple-touch-icon-152x152.png","uriCount":2},{"uri":"/images/stories/raith/wohnung_1_web.jpg","uriCount":5},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_grillplatz.jpg","uriCount":2},{"uri":"/images/stories/raith/almenland_logo.jpg","uriCount":5},{"uri":"/templates/jp_hotel/images/content_heading.gif","uriCount":6},{"uri":"/index.php?option=com_phocagallery&view=category'A=0&id=1&Itemid=53","uriCount":1},{"uri":"/dinbil.se","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_zimmer.jpg","uriCount":2},{"uri":"/images/phocagallery/Ferienwohnung_2/thumbs/phoca_thumb_m_5_bad.jpg","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_terasse.jpg","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_l_almhuette_raith_002.jpg","uriCount":1},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_003.jpg","uriCount":2},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_l_almhuette_raith_004.jpg","uriCount":1},{"uri":"/images/stories/raith/grillplatz.jpg","uriCount":5},{"uri":"/robots.txt","uriCount":18},{"uri":"/images/stories/slideshow/almhuette_raith_06.jpg","uriCount":5},{"uri":"/index.php?option=com_phocagallery&view=category&id=1&Itemid=53'A=0","uriCount":1},{"uri":"/components/com_phocagallery/assets/phocagallery.css","uriCount":3},{"uri":"/images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_005.jpg","uriCount":2},{"uri":"/components/com_phocagallery/assets/js/shadowbox/src/skin/classic/loading.gif","uriCount":3},{"uri":"/","uriCount":14},{"uri":"/apple-touch-icon-120x120.png","uriCount":4},{"uri":"/index.php?option=com_content&view=article&id=46&Itemid=54","uriCount":9}]}

```












