from tests.helpers.samples.image import SAMPLE_IMAGE_ID
from tests.helpers.samples.utils import read_emu_extract

# this is taken from ecatalogue.export.20231019.gz but with the MulMultiMediaRefs
# replaced with a single reference to the SAMPLE_IMAGE_ID
raw_data = f"""
rownum=1511
irn:1=2531732
SummaryData:1=1968.11.11.17-18, Synodontis schall (Bloch & Schneider, 1801) -- Mochokidae; Siluriformes; Actinopterygii, 2, Spirit, Mochokidae F135: DC:2B:461-469; South Kensington; Darwin Centre 1; 2; DC1.2.03; 461-469
ExtendedData:1=2531732
ExtendedData:2=
ExtendedData:3=1968.11.11.17-18, Synodontis schall (Bloch & Schneider, 1801) -- Mochokidae; Siluriformes; Actinopterygii, 2, Spirit, Mochokidae F135: DC:2B:461-469; South Kensington; Darwin Centre 1; 2; DC1.2.03; 461-469
ColDepartment:1=Zoology
ColSubDepartment:1=LS Fish
ColRecordType:1=Specimen
GeneralCatalogueNumber:1=0
MinBmNumber:1=0
EntCatSpecimenCount:1=2
EntCatBarcode:1=013234322
EntIdeTaxonRef:1=2564466
EntIdeTaxonRefLocal0:1=2564466
EntIdeScientificNameLocal:1=Synodontis schall (Bloch & Schneider, 1801)
EntIdeTaxonLocal:1=Synodontis schall (Bloch & Schneider, 1801) -- Mochokidae; Siluriformes; Actinopterygii
EntIdeNamePartsLocal:1=<genus>Synodontis</genus> <species>schall</species> <auth>Bloch & Schneider, 1801</auth>
EntIdeFamilyLocal:1=Mochokidae
EntIdeGenusLocal:1=Synodontis
EntIdeSpeciesLocal:1=schall
EntIdeOrderLocal:1=Siluriformes
EntIdeClassLocal:1=Actinopterygii
EntIdeQualifiedName:1=Synodontis schall (Bloch & Schneider, 1801)
EntIdeQualifiedNameAutomatic:1=Yes
EntIdeFiledAs:1=Yes
EntBasionymAuthorsLocal:1=Bloch
EntMostCurrentVoucherDetRef:1=2564466
EntIdeFiledAsTaxonRef:1=2564466
EntIdeFiledAsQualifiedName:1=Synodontis schall (Bloch & Schneider, 1801)
IdeFiledAsRefLocal:1=2564466
IdeFiledAsClass:1=Actinopterygii
IdeFiledAsOrder:1=Siluriformes
IdeFiledAsFamily:1=Mochokidae
IdeFiledAsGenus:1=Synodontis
IdeFiledAsSpecies:1=schall
IdeFiledAsAuthors:1=Bloch & Schneider, 1801
IdeFiledAsScientificName:1=Synodontis schall (Bloch & Schneider, 1801)
IdeCurrentTaxonRef:1=2564466
IdeCurrentQualifiedName:1=Synodontis schall (Bloch & Schneider, 1801)
IdeCurrentRefLocal:1=2564466
IdeCurrentClass:1=Actinopterygii
IdeCurrentOrder:1=Siluriformes
IdeCurrentFamily:1=Mochokidae
IdeCurrentGenus:1=Synodontis
IdeCurrentSpecies:1=schall
IdeCurrentAuthors:1=Bloch & Schneider, 1801
IdeCurrentScientificName:1=Synodontis schall (Bloch & Schneider, 1801)
EntRelIsParent:1=No
PalOthNumber:1=1968.11.11.17-18
PalOthTypeofNumber:1=Original Reg. No.
CollEventDateVisitedFrom=1968-09-16
CollEventDateVisitedTo=1968-09-16
CollEventSummary:1=16/09/1968; 16/09/1968
CollEventExpeditionName:1=Sandhurst Ethiopian Expedition 1968
PalAcqAcquisitionRef:1=2600034
PalAcqAcquisitionRefLocal:1=2600034
PalAcquisitionMethodLocal:1=Presented
PalAcqAcquisitionSummaryData:1=Presented; Sandhurst Ethiopian Expedition 1968; Expedition
PalAcqSourceLocal:1=Sandhurst Ethiopian Expedition 1968
PalAcqAccLotDonorRef:1=2500411
PalAcqAccLotDonorRefLocal:1=2500411
PalAcqAccLotDonorFullName:1=Sandhurst Ethiopian Expedition 1968
RegRegistrationNumber:1=1968.11.11.17-18
RegSortableRegistrationNumber:1=1968.0011.0011.0017-0018
RegYearOrVol:1=1968
RegMonthOrBatch:1=11
RegDay:1=11
RegSequence:1=17
RegSequenceTo:1=18
RegCode:1=PI03
RegBackRegistration:1=Yes
CatKindOfObject:1=Spirit
CatPreservative:1=IMS 70%
LabKind:1=Toner
LabCount:1=1
LabSize:1=Large
LabPrintMethod:1=Ricoh
LabDate0=2023-10-18
ColSiteRef:1=2504705
ColSiteRefLocal:1=2504705
ColSiteSummaryDataLocal:1=Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m, Metekel, Benshangul-Gumaz, Ethiopia, Blue Nile, 10 5 0.000 N, 35 38 0.000 E
ColSiteContinentLocal:1=Africa
ColSiteCountryLocal:1=Ethiopia
ColSiteRiverBasinLocal:1=Blue Nile
ColSiteProvinceTerrLocal:1=Benshangul-Gumaz
ColSiteDistictCountyLocal:1=Metekel
LocCurrentLocationRef:1=2500166
LocCurrentLocationRefLocal:1=2500166
LocCurrentSummaryData:1=Mochokidae F135: DC:2B:461-469; South Kensington; Darwin Centre 1; 2; DC1.2.03; 461-469
LocCurrentLocationLevel1Local:1=South Kensington
LocCurrentLocationLevel2Local:1=Darwin Centre 1
LocCurrentLocationLevel3Local:1=2
LocCurrentLocationLevel6Local:1=461-469
LocDateMoved=2023-10-18
LocTimeMoved=19:11:
LocIndependentlyMoveable:1=Yes
LocPermanentLocationRef:1=2500166
LocPermanentLocationRefLocal:1=2500166
LocPermanentSummaryData:1=Mochokidae F135: DC:2B:461-469; South Kensington; Darwin Centre 1; 2; DC1.2.03; 461-469
ConConditionPeriodType:1=Years
ValValuationPeriodType:1=Years
DeaTransferOfTitle:1=Unknown
sumRegistrationNumber:1=1968.11.11.17-18
sumSiteRef:1=2504705
sumSiteRefLocal:1=2504705
sumSiteSummaryData:1=Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m, Metekel, Benshangul-Gumaz, Ethiopia, Blue Nile, 10 5 0.000 N, 35 38 0.000 E
sumContinent:1=Africa
sumCountry:1=Ethiopia
sumProvinceStateTerritory:1=Benshangul-Gumaz
sumDistrictCountyShire:1=Metekel
sumPreciseLocation:1=Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m
sumPreferredCentroidLatitude=10 05 00.000 N
sumPreferredCentroidLatDec:1=10.0833333
sumPreferredCentroidLongitude=035 38 00.000 E
sumPreferredCentroidLongDec:1=35.6333333
DarInstitutionCode:1=NHMUK
DarCollectionCode:1=ZOO
DarCatalogNumber:1=1968.11.11.17-18
DarScientificName:1=Synodontis schall (Bloch & Schneider, 1801)
DarBasisOfRecord:1=Specimen
DarClass:1=Actinopterygii
DarOrder:1=Siluriformes
DarFamily:1=Mochokidae
DarGenus:1=Synodontis
DarSpecies:1=schall
DarScientificNameAuthor:1=Bloch; Schneider
DarYearCollected:1=1968
DarMonthCollected:1=9
DarDayCollected:1=16
DarCountry:1=Ethiopia
DarStateProvince:1=Benshangul-Gumaz
DarLocality:1=Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m
DarIndividualCount:1=2
DarGlobalUniqueIdentifier:1=NHMUK:ecatalogue:2531732
DarScientificNameAuthorYear:1=1801
DarContinent:1=Africa
DarWaterBody:1=Blue Nile
DarCatalogNumberText:1=1968.11.11.17-18
DarHigherGeography:1=Africa; Ethiopia; Benshangul-Gumaz; Metekel
DarDecimalLatitude:1=10.0833333
DarDecimalLongitude:1=35.6333333
DarHigherTaxon:1=Actinopterygii; Siluriformes; Mochokidae
DarOtherCatalogNumbers:1=1968.11.11.17-18
DarStartYearCollected:1=1968
DarEndYearCollected:1=1968
DarStartMonthCollected:1=9
DarEndMonthCollected:1=9
DarStartDayCollected:1=16
DarEndDayCollected:1=16
MulMultiMediaRef:1={SAMPLE_IMAGE_ID}
MulHasMultiMedia:1=Y
MulPrimaryMultiMediaRef:1={SAMPLE_IMAGE_ID}
AdmOriginalData:1=COLL. EVENT REMOVAL 2016 COLL. EVENT DATA:
AdmOriginalData:2=
AdmOriginalData:3=CE IRN:=2541029
AdmOriginalData:4=
AdmOriginalData:5=AreArea:=0.00 sq
AdmOriginalData:6=
AdmOriginalData:7=ColSiteRef:=2504705
AdmOriginalData:8=
AdmOriginalData:9=SummaryData-ColSiteLocal:=Ethiopia; 10 5 0.000 N; 35 38 0.000 E; Blue Nile
AdmOriginalData:10=
AdmOriginalData:11=
AdmOriginalData:12=MOA CATALOGUE LEGACY DATA:
AdmOriginalData:13=
AdmOriginalData:14=Gen_Alert memo = 1
AdmOriginalData:15=
AdmOriginalData:16=
AdmOriginalData:17=
AdmOriginalData:18=Gen_Remarks = 1
AdmOriginalData:19=
AdmOriginalData:20=s_Generation = 888
AdmOriginalData:21=
AdmOriginalData:22=s_Lineage = #Binary Field#
AdmOriginalData:23=
AdmOriginalData:24=s_GUID = {{guid {{CD17CC1A-B19C-11D4-B01D-0060978EF74F}} }}
AdmOriginalData:25=
AdmOriginalData:26=Reg_id = 89337
AdmOriginalData:27=
AdmOriginalData:28=Last_reg_id = 89341
AdmOriginalData:29=
AdmOriginalData:30=Register Code = PI03
AdmOriginalData:31=
AdmOriginalData:32=Registration Number = 1968.11.11.17-18
AdmOriginalData:33=
AdmOriginalData:34=Genus = Synodontis
AdmOriginalData:35=
AdmOriginalData:36=Species = schall
AdmOriginalData:37=
AdmOriginalData:38=Family = Mochokidae
AdmOriginalData:39=
AdmOriginalData:40=Country = Ethiopia
AdmOriginalData:41=
AdmOriginalData:42=River = Blue Nile
AdmOriginalData:43=
AdmOriginalData:44=locality = Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m
AdmOriginalData:45=
AdmOriginalData:46=
AdmOriginalData:47=
AdmOriginalData:48=Aquisition method = Presented
AdmOriginalData:49=
AdmOriginalData:50=Source surname = (E) Sandhurst Ethiopian Expedition 1968
AdmOriginalData:51=
AdmOriginalData:52=Literal collection date = 16/09/1968
AdmOriginalData:53=
AdmOriginalData:54=Expedition name = Sandhurst Ethiopian Expedition 1968
AdmOriginalData:55=
AdmOriginalData:56=FAO Area = 01
AdmOriginalData:57=
AdmOriginalData:58=Latitude degrees = 10
AdmOriginalData:59=
AdmOriginalData:60=Latitude minutes = 5
AdmOriginalData:61=
AdmOriginalData:62=Latitude seconds = 0
AdmOriginalData:63=
AdmOriginalData:64=Latitude Pole = N
AdmOriginalData:65=
AdmOriginalData:66=Decimal latitude = 10.0833
AdmOriginalData:67=
AdmOriginalData:68=Longitude degrees = 35
AdmOriginalData:69=
AdmOriginalData:70=Longitude minutes = 38
AdmOriginalData:71=
AdmOriginalData:72=Longitude seconds = 0
AdmOriginalData:73=
AdmOriginalData:74=Longitude Pole = E
AdmOriginalData:75=
AdmOriginalData:76=Decimal Longitude = 35.6333333333333
AdmOriginalData:77=
AdmOriginalData:78=Number of items = 2
AdmOriginalData:79=
AdmOriginalData:80=Register flag = True
AdmOriginalData:81=
AdmOriginalData:82=Specimen seen flag = False
AdmOriginalData:83=
AdmOriginalData:84=Alert = Cat-Check 27/03/2002
AdmOriginalData:85=
AdmOriginalData:86=Check = DC:2B:461-469
AdmOriginalData:87=
AdmOriginalData:88=Reg Registration Number = 1968.11.11.17-18
AdmOriginalData:89=
AdmOriginalData:90=Reg Genus = Synodontis
AdmOriginalData:91=
AdmOriginalData:92=Reg Species = schall
AdmOriginalData:93=
AdmOriginalData:94=Classification = F
AdmOriginalData:95=
AdmOriginalData:96=Loan = False
AdmOriginalData:97=
AdmOriginalData:98=Orden = 1968111100017
AdmPublishWebNoPasswordFlag:1=Y
AdmPublishWebNoPassword:1=Yes
AdmPublishWebPasswordFlag:1=Y
AdmPublishWebPassword:1=Yes
AdmWebMetadata:1=0 Ethiopia Benshangul-Gumaz Metekel
AdmGUIDPreferredType:1=UUID4
AdmGUIDPreferredValue:1=4ffbec35-4397-440b-b781-a18b4d958cef
AdmGUIDIsPreferred:1=Yes
AdmGUIDType:1=UUID4
AdmGUIDValue:1=4ffbec35-4397-440b-b781-a18b4d958cef
AdmInsertedBy:1=KE EMu Administrator
AdmDateInserted=2003-06-20
AdmTimeInserted=09:24:24.000
AdmModifiedBy:1=Rupert Collins
AdmDateModified=2023-10-18
AdmTimeModified=19:11:01.000
AdmDateRecordModified=2023-10-18
AdmTimeRecordModified=19:11:01.000
SecCanDisplay:1=Group Default
SecCanEdit:1=Group Default
SecCanDelete:1=Group Default
SecDepartment:1=Zoology
SecLookupRoot:1=Zoology
"""

SAMPLE_SPECIMEN_ID, SAMPLE_SPECIMEN_DATA = read_emu_extract(raw_data)
