from tests.helpers.dumps import read_emu_extract
from tests.helpers.samples.specimen import SAMPLE_SPECIMEN_ID

# this is taken from ecatalogue.export.20231008.gz but with the EntPreSpecimenRef field
# replaced with a single reference to the SAMPLE_SPECIMEN_ID
raw_data = f"""
rownum=3645
irn:1=9968955
SummaryData:1=no Collection Kind for preparation (irn 9968955)
ExtendedData:1=9968955
ExtendedData:2=
ExtendedData:3=no Collection Kind for preparation (irn 9968955)
ColDepartment:1=Zoology
ColSubDepartment:1=Molecular Collections
ColRecordType:1=Preparation
GeneralCatalogueNumber:1=irn: 9968955
EntIdeQualifiedNameAutomatic:1=Yes
EntPreSpecimenRef:1={SAMPLE_SPECIMEN_ID}
EntPreSpecimenRefLocal:1={SAMPLE_SPECIMEN_ID}
EntPreSpecimenTaxonLocal:1=Eurythenes maldoror d'Udekem d'Acoz & Havermans, 2015 -- Eurytheneidae; Amphipoda; Malacostraca
EntPreSpecimenTaxonLocalRef:1=790675
EntPreCatalogueNumberLocal:1=014453676
EntPreContents:1=**OTHER_SOMATIC_ANIMAL_TISSUE**
EntPrePreparationKind:1=DNA Extract
EntPrePreparatorRef:1=406667
EntPrePreparatorRefLocal:1=406667
EntPrePreparatorSumDataLocal:1=Chris Fletcher; Natural History Museum; Life Sciences; Fletcher
EntPreDate=2022-05-09
EntPreNumber:1=C9K02TWP_B2
EntPreTaxonSummaryDataLocal:1=Eurythenes maldoror d'Udekem d'Acoz & Havermans, 2015 -- Eurytheneidae; Amphipoda; Malacostraca
EntPreSpecimenCatNumLocal:1=014453676
EntPreSpecimenPresLocal:1=Dry frozen (-80°C)
AcqHistoric:1=No
LocIndependentlyMoveable:1=Yes
AcqLegTransferOfTitle:1=No
AcqLegPurAgree:1=No
AcqLegConfirmationOfGift:1=No
AcqLegDueDilligence:1=No
AcqLegCollectionImpact:1=No
NteText0:1=S
NteText1:1=Purpose of specimen: DNA barcoding only
NteText2:1=Pleopod
NteType:1=Size
AdmPublishWebNoPasswordFlag:1=Y
AdmPublishWebNoPassword:1=Yes
AdmPublishWebPasswordFlag:1=Y
AdmPublishWebPassword:1=Yes
AdmGUIDPreferredType:1=UUID4
AdmGUIDPreferredValue:1=f11c9c35-4da5-45e5-9dbb-6f8f55b26aa7
AdmGUIDIsPreferred:1=Yes
AdmGUIDType:1=UUID4
AdmGUIDValue:1=f11c9c35-4da5-45e5-9dbb-6f8f55b26aa7
AdmInsertedBy:1=Heather Allen
AdmDateInserted=2022-09-12
AdmImportIdentifier:1=12092022_JC231_Prep
AdmTimeInserted=17:07:51.000
AdmSystemIdentifier:1=heata2-220912-1706
AdmModifiedBy:1=Heather Allen
AdmDateModified=2022-09-12
AdmTimeModified=17:21:14.000
AdmDateRecordModified=2023-10-06
AdmTimeRecordModified=15:01:03.000
SecRecordStatus:1=Active
SecCanDisplay:1=Group Default
SecCanEdit:1=Group Default
SecCanDelete:1=Group Default
SecDepartment:1=Entomology
SecLookupRoot:1=Entomology
NhmSecOpenDataPolicyException:1=none
NhmSecProjectName:1=Darwin Tree of Life
"""

SAMPLE_PREPARATION_ID, SAMPLE_PREPARATION_DATA = read_emu_extract(raw_data)
