#/usr/bin/python
import random
import string
import json

#/route -> schedule, hotel -> review, hotel -> public_likes

cfg = {
	   "nqueries": 		1,
		#over arching document types
	   "sql1name": 		["accts","cont","copasses","demos","name"],
		#non array fields to choose from
	   "sql1":			["linkflds","linkflds","linkflds","linkflds","linkflds"],
		#nonarray fields
		"linkflds":		["owner","type"],
		#array fields available to choose from
		"accts":		["mems","profids"],
		"cont":			["Emails","Phones","Addresses"],
		"copasses":		["occur"], #need to add fullname field once variety of field is lessened
		"demos":		["bdayinfo","gen"],
		"name":			["names","prefixes"],
		#non array fields
		"mems": 		["owner","type"],
		"profids":		["owner","type"],
		"Emails":		["owner","type"],
		"Phones":		["owner","type"],
		"Addresses":	["owner","type"],
		"occur": 		["owner", "type"],
		"bdayinfo": 	["owner", "type"],
		"gen": 			["owner", "type"],
		"names": 		["owner", "type"],
		"prefixes": 	["owner", "type"],
	}
# fields inside of the arrays
objectElements = {
		"mems":			["memid","progid","validated"],
		"profids":		["id","type"],
		"Emails":		["Emails"],
		"Phones":		["Country", "Number"],
		"Addresses":	["country","zip","reason"],
		# need to lessen variety of this field"fullname":		["fullname"],
		"occur":		["occur"],
		"bdayinfo":		["day","mon","year"],
		"gen":			["gen"],
		"names":		["first","last"],
		"prefixes":		["prefixes"],
		}

# values of specific fields inside of the arrays
fieldMap = {
		"memid":		["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946",'947',"948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999"],
		"progid":		["\"AA\"","\"BB\"","\"CC\"","\"DD\"","\"EE\"","\"FF\"","\"GG\"","\"HH\"","\"II\"","\"JJ\"","\"KK\"","\"LL\"","\"MM\"","\"NN\"","\"OO\"","\"PP\"","\"QQ\"","\"RR\"","\"SS\"","\"TT\"","\"UU\"","\"VV\"","\"WW\"","\"XX\"","\"YY\"","\"ZZ\"","\"BE\"","\"AB\"","\"CD\"","\"EF\"","\"JK\"","\"LM\"","\"NO\"","\"PQ\"","\"RS\"","\"TU\"","\"VW\"","\"XY\""],
		"validated":	["true","false"],
		"id":			["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140","141","142","143","144","145","146","147","148","149","150","151","152","153","154","155","156","157","158","159","160","161","162","163","164","165","166","167","168","169","170","171","172","173","174","175","176","177","178","179","180","181","182","183","184","185","186","187","188","189","190","191","192","193","194","195","196","197","198","199","200","201","202","203","204","205","206","207","208","209","210","211","212","213","214","215","216","217","218","219","220","221","222","223","224","225","226","227","228","229","230","231","232","233","234","235","236","237","238","239","240","241","242","243","244","245","246","247","248","249","250","251","252","253","254","255","256","257","258","259","260","261","262","263","264","265","266","267","268","269","270","271","272","273","274","275","276","277","278","279","280","281","282","283","284","285","286","287","288","289","290","291","292","293","294","295","296","297","298","299","300","301","302","303","304","305","306","307","308","309","310","311","312","313","314","315","316","317","318","319","320","321","322","323","324","325","326","327","328","329","330","331","332","333","334","335","336","337","338","339","340","341","342","343","344","345","346","347","348","349","350","351","352","353","354","355","356","357","358","359","360","361","362","363","364","365","366","367","368","369","370","371","372","373","374","375","376","377","378","379","380","381","382","383","384","385","386","387","388","389","390","391","392","393","394","395","396","397","398","399","400","401","402","403","404","405","406","407","408","409","410","411","412","413","414","415","416","417","418","419","420","421","422","423","424","425","426","427","428","429","430","431","432","433","434","435","436","437","438","439","440","441","442","443","444","445","446","447","448","449","450","451","452","453","454","455","456","457","458","459","460","461","462","463","464","465","466","467","468","469","470","471","472","473","474","475","476","477","478","479","480","481","482","483","484","485","486","487","488","489","490","491","492","493","494","495","496","497","498","499","500","501","502","503","504","505","506","507","508","509","510","511","512","513","514","515","516","517","518","519","520","521","522","523","524","525","526","527","528","529","530","531","532","533","534","535","536","537","538","539","540","541","542","543","544","545","546","547","548","549","550","551","552","553","554","555","556","557","558","559","560","561","562","563","564","565","566","567","568","569","570","571","572","573","574","575","576","577","578","579","580","581","582","583","584","585","586","587","588","589","590","591","592","593","594","595","596","597","598","599","600","601","602","603","604","605","606","607","608","609","610","611","612","613","614","615","616","617","618","619","620","621","622","623","624","625","626","627","628","629","630","631","632","633","634","635","636","637","638","639","640","641","642","643","644","645","646","647","648","649","650","651","652","653","654","655","656","657","658","659","660","661","662","663","664","665","666","667","668","669","670","671","672","673","674","675","676","677","678","679","680","681","682","683","684","685","686","687","688","689","690","691","692","693","694","695","696","697","698","699","700","701","702","703","704","705","706","707","708","709","710","711","712","713","714","715","716","717","718","719","720","721","722","723","724","725","726","727","728","729","730","731","732","733","734","735","736","737","738","739","740","741","742","743","744","745","746","747","748","749","750","751","752","753","754","755","756","757","758","759","760","761","762","763","764","765","766","767","768","769","770","771","772","773","774","775","776","777","778","779","780","781","782","783","784","785","786","787","788","789","790","791","792","793","794","795","796","797","798","799","800","801","802","803","804","805","806","807","808","809","810","811","812","813","814","815","816","817","818","819","820","821","822","823","824","825","826","827","828","829","830","831","832","833","834","835","836","837","838","839","840","841","842","843","844","845","846","847","848","849","850","851","852","853","854","855","856","857","858","859","860","861","862","863","864","865","866","867","868","869","870","871","872","873","874","875","876","877","878","879","880","881","882","883","884","885","886","887","888","889","890","891","892","893","894","895","896","897","898","899","900","901","902","903","904","905","906","907","908","909","910","911","912","913","914","915","916","917","918","919","920","921","922","923","924","925","926","927","928","929","930","931","932","933","934","935","936","937","938","939","940","941","942","943","944","945","946",'947',"948","949","950","951","952","953","954","955","956","957","958","959","960","961","962","963","964","965","966","967","968","969","970","971","972","973","974","975","976","977","978","979","980","981","982","983","984","985","986","987","988","989","990","991","992","993","994","995","996","997","998","999"],
		"type":			["\"shopfc\"","\"shopa\"","\"shopb\"","\"shopc\"","\"shopd\"","\"shope\"","\"shopf\"","\"shopg\"","\"shoph\"","\"shopi\"","\"shopj\"","\"shopk\"","\"shopl\"","\"shopm\"","\"shopn\"","\"shopo\"","\"shopp\"","\"shopq\"","\"shopr\"","\"shops\"","\"shopt\"","\"shopu\"","\"shopv\"","\"shopw\"","\"shopx\"","\"shopy\"","\"shopz\""],
		"Emails":		["\"Jody_Okuneva@hotmail.com\"","\"Alexis69@hotmail.com\"","\"Angeline.Moore@gmail.com\"","\"Terence_Dach@hotmail.com\"","\"Madalyn_Mann@yahoo.com\"","\"Herminio13@hotmail.com\"","\"Haleigh_Romaguera@hotmail.com\"","\"Imelda.Roberts@yahoo.com\"","\"Heaven80@hotmail.com\"","\"Demario.Ziemann@gmail.com\"","\"Kelsi_Hessel49@hotmail.com\"","\"Helene_Beahan96@gmail.com\"","\"Verda_Fisher59@hotmail.com\"","\"Pat20@gmail.com\"","\"Vella_Stiedemann60@yahoo.com\"","\"Braulio_Mann8@gmail.com\"","\"Gwendolyn_Rosenbaum@gmail.com\"","\"Renee13@gmail.com\"","\"Giuseppe31@gmail.com\"","\"Stone_Stark@gmail.com\"","\"Kristofer.Huels39@yahoo.com\"","\"Terrell96@yahoo.com\"","\"Dallin.OReilly@yahoo.com\"","\"Oceane_Zboncak26@yahoo.com\"","\"Ila_Tillman@gmail.com\"","\"Akeem31@yahoo.com\"","\"Florence.Cummings@hotmail.com\"","\"Nathanial_Strosin@hotmail.com\"","\"Dustin_Reilly@gmail.com\"","\"Joesph_Reichert@hotmail.com\"","\"Rudy.Parker7@hotmail.com\"","\"Selina.Batz52@yahoo.com\"","\"Heber.Schmidt@gmail.com\"","\"Rhianna.Lakin@yahoo.com\"","\"Remington.Yundt43@gmail.com\"","\"Tiana34@gmail.com\"","\"Alfonso.Jakubowski@gmail.com\"","\"Narciso55@hotmail.com\"","\"Daisha81@hotmail.com\"","\"Carlee76@gmail.com\"","\"Eduardo.Halvorson@gmail.com\"","\"Magdalen_Kozey39@hotmail.com\"","\"Bernhard_Romaguera72@hotmail.com\"","\"Nia.Wilderman42@gmail.com\"","\"Liliane.Hand65@gmail.com\"","\"Bailey.Farrell67@yahoo.com\"","\"Camille92@hotmail.com\"","\"Joseph3@hotmail.com\"","\"Jude97@hotmail.com\"","\"Lonny.Schoen47@gmail.com\""],
		"Country":		["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99"],
		"Number":		["\"1-961-612-1069\"","\"301-476-3669\"","\"(565) 710-6462\"","\"1-378-034-6779\"","\"492-985-6135\"","\"178-152-6156\"","\"1-076-708-2779\"","\"146.424.0406\"","\"335.703.8259\"","\"479.790.3611\"","\"1-032-597-9268\"","\"213.448.4491\"","\"(543) 398-4171\"","\"1-475-834-8437\"","\"536.048.0852\"","\"721.711.5286\"","\"990-368-8911\"","\"1-828-172-6938\"","\"832-246-4210\"","\"(697) 544-6144\"","\"241-750-5423\"","\"492.126.5418\"","\"(802) 778-8764\"","\"1-133-875-3528\"","\"729-713-8800\"","\"415-473-8557\"","\"(454) 392-5976\"","\"(100) 785-3847\"","\"1-207-722-8885\"","\"(841) 328-7487\"","\"054-288-6421\"","\"(195) 831-2698\"","\"1-599-255-8770\"","\"(159) 765-1915\"","\"(429) 093-5927\"","\"363.960.4462\"","\"832.947.8291\"","\"656.608.4715\"","\"(552) 554-2339\"","\"(214) 343-6839\"","\"1-098-228-0967\"","\"(884) 141-6115\"","\"530.017.8057\"","\"1-516-549-2023\"","\"042-399-2622\"","\"898.787.8451\"","\"762-680-2143\"","\"259-334-5397\"","\"(998) 954-3235\"","\"(324) 257-9003\"","\"486-800-8754\"","\"146-718-6559\"","\"175.180.3383\"","\"182.562.8368\"","\"1-810-069-0112\"","\"(602) 609-7054\"","\"1-360-901-6554\"","\"775.092.8895\"","\"796-906-8284\"","\"219-042-8919\"","\"725.667.4215\"","\"1-479-588-2251\"","\"1-214-438-2028\"","\"290-310-1647\"","\"1-055-898-4535\"","\"1-620-086-2117\"","\"(346) 446-7162\"","\"920-620-2995\""],
		"country":		["\"DZ\"","\"AR\"","\"AM\"","\"AU\"","\"AT\"","\"AZ\"","\"A2\"","\"BD\"","\"BY\"","\"BE\"","\"BA\"","\"BR\"","\"BN\"","\"BG\"","\"CA\"","\"IC\"","\"CN\"","\"HR\"","\"CY\"","\"CZ\"","\"DK\"","\"EN\"","\"EE\"","\"FO\"","\"FI\"","\"FR\"","\"GE\"","\"DE\"","\"GR\"","\"GL\"","\"GU\"","\"GG\"","\"HO\"","\"HU\"","\"IN\"","\"ID\"","\"IL\"","\"IT\"","\"JP\"","\"JE\"","\"KZ\"","\"KR\"","\"KO\"","\"KG\"","\"LV\"","\"LI\"","\"LT\"","\"LU\"","\"MK\"","\"MG\"","\"M3\"","\"MY\"","\"MH\"","\"MQ\"","\"YT\"","\"MX\"","\"MN\"","\"ME\"","\"NL\"","\"NZ\"","\"NB\"","\"NO\"","\"PK\"","\"PH\"","\"PL\"","\"PO\"","\"PT\"","\"PR\"","\"RE\"","\"RU\"","\"SA\"","\"SF\"","\"CS\"","\"SG\"","\"SK\"","\"SI\"","\"ZA\"","\"ES\"","\"LK\"","\"NT\"","\"SX\"","\"UV\"","\"VL\"","\"SE\"","\"CH\"","\"TW\"","\"TJ\"","\"TH\"","\"TU\"","\"TN\"","\"TR\"","\"TM\"","\"VI\"","\"UA\"","\"GB\"","\"US\"","\"UY\"","\"UZ\"","\"VA\"","\"VN\"","\"WL\"","\"YA\""],
		"zip":			["\"17042\"","\"22015\"","\"70001\"","\"33054\"","\"60139\"","\"70806\"","\"51106\"","\"19464\"","\"30039\"","\"60014\"","\"98604\"","\"37849\"","\"14534\"","\"11793\"","\"02895\"","\"28303\"","\"52761\"","\"33404\"","\"46112\"","\"32703\"","\"20707\"","\"13760\"","\"83301\"","\"02155\"","\"11722\"","\"02169\"","\"55303\"","\"02150\"","\"06851\"","\"02072\"","\"11701\"","\"07740\"","\"37343\"","\"32714\"","\"21784\"","\"95127\"","\"49085\"","\"48192\"","\"19320\"","\"10562\"","\"20886\"","\"06489\"","\"30038\"","\"07052\"","\"23601\"","\"50310\"","\"32746\"","\"11372\"","\"65401\"","\"26508\"","\"23803\"","\"31061\"","\"32566\"","\"40004\"","\"46037\"","\"30083\"","\"11741\"","\"33801\"","\"07030\"","\"60010\"","\"20743\"","\"54935\"","\"81001\"","\"22191\"","\"31904\"","\"47802\"","\"44070\"","\"39208\"","\"10550\"","\"80911\"","\"45011\"","\"04103\"","\"60193\"","\"34668\"","\"37086\"","\"19026\"","\"38632\"","\"44077\"","\"23693\"","\"52722\"","\"33308\"","\"06405\"","\"29063\"","\"20748\"","\"21921\"","\"23832\"","\"27330\"","\"11566\"","\"76110\"","\"01876\"","\"32955\"","\"44221\"","\"60426\"","\"50401\"","\"28079\"","\"07747\"","\"01923\"","\"20850\"","\"06473\"","\"20782\""],
		"reason":		["\"Bill\"","\"Dest\"","\"Depart\""],
		# need to fix the generation of this to have less variety, currently there are 10k combinations of fullname "fullname":		[],
		"occur":		["1","2","3","4","5"],
		"day":			["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30"],
		"mon":			["1","2","3","4","5","6","7","8","9","10","11","12"],
		"year":			["1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005"],
		"gen":			["\"Male\"", "\"Female\"", "\"N/A\""],
		"first":		["\"Adele\"","\"Mandy\"","\"Fannie\"","\"Trever\"","\"Brown\"","\"Elizabeth\"","\"Bart\"","\"Columbus\"","\"Rylee\"","\"Cathy\"","\"Valentine\"","\"Cathy\"","\"Armand\"","\"Lysanne\"","\"Tyrel\"","\"Barton\"","\"Velva\"","\"Deja\"","\"Jana\"","\"Leta\"","\"Maria\"","\"Blake\"","\"Kaia\"","\"Darrick\"","\"Priscilla\"","\"Elinore\"","\"Wyman\"","\"Bradly\"","\"Freda\"","\"Rickey\"","\"Christina\"","\"Zachary\"","\"Madeline\"","\"Daisha\"","\"Rusty\"","\"Madyson\"","\"Lorna\"","\"Carmine\"","\"Casandra\"","\"Amani\"","\"Ezra\"","\"Robyn\"","\"Rick\"","\"Lora\"","\"Wiley\"","\"Tanner\"","\"Alicia\"","\"Darrion\"","\"Naomi\"","\"Lexus\"","\"Hiram\"","\"Isidro\"","\"Nellie\"","\"Cornelius\"","\"Nicole\"","\"Alec\"","\"Daisy\"","\"Roxanne\"","\"Maurine\"","\"Leopoldo\"","\"Columbus\"","\"Hallie\"","\"Vivianne\"","\"Lowell\"","\"Ramon\"","\"Julianne\"","\"Edwin\"","\"Justine\"","\"Leola\"","\"Giovanni\"","\"Trudie\"","\"Junius\"","\"Braulio\"","\"Antone\"","\"Alison\"","\"Stevie\"","\"Judd\"","\"Eldora\"","\"Hulda\"","\"Valentine\"","\"Theodore\"","\"Sheridan\"","\"Quincy\"","\"Haylee\"","\"Maida\"","\"Alvena\"","\"Nova\"","\"Schuyler\"","\"Noemy\"","\"Alva\"","\"Lisandro\"","\"Marcelino\"","\"Arvid\"","\"Daphnee\"","\"Barry\"","\"Adolph\"","\"Aurore\"","\"Ana\"","\"Kamron\"","\"Loraine\""],
		"last":			["\"Parisian\"","\"McCullough\"","\"McDermott\"","\"Mann\"","\"Bernhard\"","\"Lubowitz\"","\"Ward\"","\"Rempel\"","\"Quitzon\"","\"Monahan\"","\"VonRueden\"","\"Stracke\"","\"McGlynn\"","\"Fadel\"","\"Zieme\"","\"Schultz\"","\"Bednar\"","\"Yost\"","\"Anderson\"","\"Bashirian\"","\"Quigley\"","\"Ziemann\"","\"D'Amore\"","\"Gislason\"","\"Hayes\"","\"Bernhard\"","\"Hayes\"","\"Friesen\"","\"Pouros\"","\"Schumm\"","\"Moen\"","\"Daugherty\"","\"Wehner\"","\"Brekke\"","\"Kshlerin\"","\"Boyle\"","\"Miller\"","\"Murphy\"","\"Koelpin\"","\"Kulas\"","\"Beier\"","\"Smitham\"","\"Lindgren\"","\"Grady\"","\"Swaniawski\"","\"Crist\"","\"West\"","\"White\"","\"Anderson\"","\"Yost\"","\"Okuneva\"","\"Stiedemann\"","\"Pacocha\"","\"McDermott\"","\"Monahan\"","\"Hane\"","\"Larson\"","\"Koss\"","\"Kuvalis\"","\"McKenzie\"","\"Hills\"","\"Denesik\"","\"Quitzon\"","\"Nolan\"","\"Weber\"","\"Walter\"","\"Gutkowski\"","\"Trantow\"","\"Harris\"","\"Stoltenberg\"","\"Feeney\"","\"Larkin\"","\"Waters\"","\"Smith\"","\"O'Conner\"","\"MacGyver\"","\"Pacocha\"","\"Schoen\"","\"Goodwin\"","\"Bergstrom\"","\"Bogan\"","\"Thiel\"","\"Leffler\"","\"Monahan\"","\"Steuber\"","\"Boehm\"","\"Pfeffer\"","\"Abshire\"","\"Balistreri\"","\"Robel\"","\"Swift\"","\"Schaden\"","\"Wisoky\"","\"McDermott\"","\"Bosco\"","\"Bruen\"","\"Pouros\"","\"Fadel\"","\"Dibbert\"","\"Kuvalis\""],
		"prefixes":		["\"MR\"", "\"MS\"", "\"MRS\""],
}

class ArrayGeneratorLinks(object):
	def common_field(self,i, table2):
		li = []
		for f1 in cfg[cfg["sql1"][i]]:
			for f2 in cfg[table2]:
				if f1 == f2:
					li.append(f1)
		return li


	def projection(self,t, fields):
		projs = random.sample(fields, random.randint(1, len(fields)))
		projs.insert(0, "id")
		newprojs = []
		for p in projs:
			p = t + "." + p
			newprojs.append(p)
		seperator = ", "
		return seperator.join(projs), "DISTINCT " + seperator.join(newprojs)


	def betweenclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		start = random.randint(0, len(fields) - 2)
		end = random.randint(start, len(fields) - 1)
		s1 = table + "." + field + " between " + fields[start] + " AND " + fields[end]
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " between " + fields[start] + " AND " + fields[end] + " END"
		else :
			s2 += alias + "." + field + " between " + fields[start] + " AND " + fields[end] + " END"
		array.append(s2)
		return array

	def likeclause_leading(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		l = random.randint(1, len(fields[idx]) - 1)
		if l == 1:
			l = l + 1
		prex = fields[idx][:l] + "%"
		s1 = table + "." + field + " like " + prex + "\""
		array.append(s1)
		s2 = ""
		if field == "public_likes":
			s2 += alias + " like " + str(prex) + "\" END"
		else :
			s2 += alias + "." + field + " like " + str(prex) + "\" END"
		array.append(s2)
		return array


	def likeclause_noleading(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		l = random.randint(1, len(fields[idx]))
		if l == 1:
			l = l + 1
		prex =  "%" + fields[idx][:l] + "%"
		s1 = table + "." + field + " like " + prex
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " like " + prex + "\" END"
		else :
			s2 += alias + "." + field + " like " + prex + "\" END"
		array.append(s2)
		return array


	def equclause(self,table, field, alias):
		array = []
		#idxfield passed in from top level in our case fieldmap['day flight utc']
		fields = fieldMap[field]
		# pick a random value for the selected field
		idx = random.randint(0, len(fields) - 1)
		s1 = table + "." + field + " = "  + fields[idx]
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " = " + fields[idx] + " END"
		else :
			# s.field = field value
			s2 += alias + "." + field + " = " + fields[idx] + " END"
		array.append(s2)
		return array

	def multiequclause(self, table, field, field2, alias):
		array = []
		if random.randint(0, 100) < 80:
			conjunction = " AND "
		else:
			conjunction = " OR "
		fields = fieldMap[field]
		fields2 = fieldMap[field2]
		idx = random.randint(0, len(fields) - 1)
		idx2 = random.choice([i for i in range(0,len(fields2) - 1) if i != idx])
		s1 = table + "." + field + " = "  + fields[idx]
		array.append(s1)
		s2 = ""
		s2 += alias + "." + field + " = " + fields[idx] + conjunction + alias + "." + field2 + " = " + fields2[idx2] + " END"
		array.append(s2)
		return array

	def lessclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		s1 = table + "." + field + " < "  + fields[idx]
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " < " + fields[idx] + " END"
		else :
			s2 += alias + "." + field + " < " + fields[idx] + " END"
		array.append(s2)
		return array

	def largerclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		idx = random.randint(0, len(fields) - 1)
		s1 = table + "." + field + " > "  + fields[idx]
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " > " + fields[idx] + " END"
		else :
			s2 += alias + "." + field + " > " + fields[idx] + " END"
		array.append(s2)
		return array

	def less_array(self,table, field1, field2, alias):
		array = []
		fields_1 = fieldMap[field1]
		fields_2 = fieldMap[field2]
		idx_1 = random.randint(0, len(fields_1) - 1)
		idx_2 = random.randint(0, len(fields_2) - 1)
		s1 = table + "." + field1 + " < "  + fields_1[idx_1] + " OR (" + table + "." + field1 + " = "  + fields_1[idx_1] + " AND " + table + "." + field2 + " < " + fields_2[idx_2] + ")"
		array.append(s1)
		s2 = "[" + alias + "." + field1 + ", " + alias + "." + field2  + "] < [" + fields_1[idx_1] + ", " + fields_2[idx_2] + "]" + " END"
		array.append(s2)
		return array

	def larger_array(self,table, field1, field2, alias):
		array = []
		fields_1 = fieldMap[field1]
		fields_2 = fieldMap[field2]
		idx_1 = random.randint(0, len(fields_1) - 1)
		idx_2 = random.randint(0, len(fields_2) - 1)
		s1 = table + "." + field1 + " > "  + fields_1[idx_1] + " OR (" + table + "." + field1 + " = "  + fields_1[idx_1] + " AND " + table + "." + field2 + " > " + fields_2[idx_2] + ")"
		array.append(s1)
		s2 = "[" + alias + "." + field1 + ", " + alias + "." + field2  + "] > [" + fields_1[idx_1] + ", " + fields_2[idx_2] + "]" + " END"
		array.append(s2)
		return array


	def inclause(self,table, field, alias):
		array = []
		fields = fieldMap[field]
		sub = random.sample(fields, random.randint(1, len(fields)))
		seperator = ", "
		l = seperator.join(sub)
		s1 = table + "." + field + " in (" + l + ")"
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " in [" + l + "] END"
		else :
			s2 += alias + "." + field + " in [" + l + "] END"
		array.append(s2)
		return array

	def withinclause(self, table, field, alias):
		array = []
		object = {}
		for entry in fieldMap:
			if entry != field :
				fields = fieldMap[entry]
				if random.randint(0, 100) < 50:
					if entry == "validated":
						object[entry] = random.choice([True,False])
					else:
						object[entry] = eval("[" + ','.join(random.sample(fields, random.randint(1, len(fields)))) + "]")
		if random.randint(0,100) < 80:
			fields = fieldMap[field]
			sub = random.sample(fields, random.randint(1, len(fields)))
			seperator = ","
			l = seperator.join(sub)
			object[field] = eval("[" + l + "]")
		else:
			fields = fieldMap[field]
			sub = random.sample(fields, random.randint(1, len(fields)))
			seperator = ", "
			l = seperator.join(sub)
			object[field] = {"nest": eval("[" + l + "]")}
		s1 =""
		array.append(s1)
		s2 = ""
		if field == "public_likes" :
			s2 += alias + " within " + json.dumps(object) + " END"
		else :
			s2 += alias + "." + field + " within " + json.dumps(object) + " END"
		array.append(s2)
		return array

	def generate_statement(self,i, table2, cond, array):
		table1 = cfg["sql1name"][0]
		fields = cfg[cfg["sql1"][0]]
		idx = random.randint(0,len(fieldMap["progid"]) - 1)
		owner_field = fieldMap['progid'][idx]
		num = random.randint(0,len(fields)-1)
		proj, newproj = self.projection(table1, fields)
		alias = table2[:1]
		s1 = ""
		s1 += "SELECT " + newproj
		s1 += " FROM " + table1
		s1 += " JOIN " + table2
		s1 += " ON " + table1 + "." + cond + " = " + table2 + "." + cond
		s1 += " AND " + table1 + ".id" " = " +  table2 + ".id"
		s1 += " WHERE " + array[0] + ";"

		proj_field_list = proj.split(",")
		order_by_list = ""
		i = 0
		ident_field = ""
		for names in cfg["sql1name"]:
			if table2 in cfg[names]:
				ident_field = names
		# Randomly add ASC or DESC to the order by fields
		for field in proj_field_list:
			if random.randint(0, 100) < 20:
				if random.randint(0,100) < 30:
					new_proj_field = field + " ASC"
				else:
					new_proj_field = field + " DESC"
				if i == 0:
					order_by_list += new_proj_field
				else:
					order_by_list += (", " + new_proj_field)
				i +=1
			else:
				if i == 0:
					order_by_list += field
				else:
					order_by_list += (", " + field)
				i += 1
		if random.randint(0, 100) < 80:
			array_type = random.choice(["ANY ","SOME "])
		else:
			array_type = "EVERY "
		if random.randint(0,100) < 10:
			if not (ident_field == "demos" or ident_field == "copasses"):
				s2 = "SELECT " + proj + " FROM `default` WHERE " + array_type + alias + " IN " + "idents." + ident_field + "." + table2 + " SATISFIES " + array[1] + " AND type = 'links' AND owner = %s ORDER BY " % owner_field + order_by_list
			else:
				s2 = "SELECT " + proj + " FROM `default` WHERE " + array_type + alias + " IN " + "idents." + ident_field + " SATISFIES " + array[1] + " AND type = 'links' AND owner = %s ORDER BY " % owner_field + order_by_list
		else:
			if not (ident_field == "demos" or ident_field == "copasses"):
				s2 = "SELECT " + proj + " FROM `default` WHERE " + array_type + alias + " IN " + "idents." + ident_field + "." + table2 + " SATISFIES " + array[1] + " AND type = 'links' ORDER BY " + order_by_list
			else:
				s2 = "SELECT " + proj + " FROM `default` WHERE " + array_type + alias + " IN " + "idents." + ident_field + " SATISFIES " + array[1] + " AND type = 'links' ORDER BY " + order_by_list

		return s1, s2


	def generate_query_pairs(self):
		global cnt
		query_array = []
		for i in xrange(0, len(cfg["sql1name"])):
			t1 = cfg["sql1name"][i]
			for t2 in cfg[t1]:
				joinconds = self.common_field(i, t2)
				array_condition = []
				alias = t2[:1]
				for cond in joinconds:
					idx = random.randint(0, len(objectElements[t2]) - 1)
					idxfield = objectElements[t2][idx]
					if len(objectElements[t2]) - 1 == 0:
						idx2 = idx
					elif not (len(objectElements[t2]) - 1 == 1):
						idx2 = random.choice([i for i in range(0,(len(objectElements[t2]) - 1)) if i != idx])
					else:
						if idx == 0:
							idx2 = 1
						else:
							idx2 = 0
					idxfield2 = objectElements[t2][idx2]
					# 60% chance of having just an equality clause
					if random.randint(0,100) < 40 or idx == idx2:
						cond_equal = self.equclause(t2, idxfield, alias)
					else:
						cond_equal = self.multiequclause(t2, idxfield, idxfield2, alias)
					array_condition.append(cond_equal)
					cond_in = self.inclause(t2, idxfield, alias)
					array_condition.append(cond_in)
					if (not idxfield == "validated") and random.randint(0,100) < 20:
						cond_within = self.withinclause(t2, idxfield, alias)
						array_condition.append(cond_within)
					cond_btw = self.betweenclause(t2, idxfield, alias)
					array_condition.append(cond_btw)
					# Like cluase needs to be on a string field
					if idxfield != "memid" and idxfield != "id" and idxfield != "Country" and idxfield != "occur" and idxfield != "day" and idxfield != "mon" and idxfield != "year" and idxfield != "validated":
						cond_like = self.likeclause_leading(t2, idxfield, alias)
						array_condition.append(cond_like)
					idx2 = random.randint(0, len(objectElements[t2]) - 1)
					idxfield2 = objectElements[t2][idx2]
					cond_larger_array = self.larger_array(t2, idxfield, idxfield2, alias)
					cond_less_array = self.less_array(t2, idxfield, idxfield2, alias)
					array_condition.append(cond_larger_array)
					array_condition.append(cond_less_array)
					for j in xrange(0, len(array_condition)):
						s1, s2 = self.generate_statement(i, t2, cond, array_condition[j])
						query_array.append(s2)
						cnt = cnt + 1
		return query_array

cnt = 0
def main():
	print("Here")
	final_list = []
	for i in xrange(cfg["nqueries"]):
		query_array = ArrayGeneratorLinks().generate_query_pairs()
		final_list += query_array
	global cnt
	print(cnt)


if __name__ == "__main__":
	random.seed()
	main()

