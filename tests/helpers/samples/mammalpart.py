from tests.helpers.dumps import read_emu_extract
from tests.helpers.samples.specimen import SAMPLE_SPECIMEN_ID

# this is taken from ecatalogue.export.20240109.gz but with the RegRegistrationParentRef
# replaced with a single reference to the SAMPLE_SPECIMEN_ID (which isn't a mammal group
# parent but this is fine)
raw_data = f"""
rownum=3815
irn:1=9547055
SummaryData:1=ZD 1887.2.10.1, Bos javanicus javanicus d'Alton, 1823, Skull, Skull, G1; Wandsworth; Wandsworth; B; WA.B.04; 5; 8
ExtendedData:1=9547055
ExtendedData:2=
ExtendedData:3=ZD 1887.2.10.1, Bos javanicus javanicus d'Alton, 1823, Skull, Skull, G1; Wandsworth; Wandsworth; B; WA.B.04; 5; 8
ColDepartment:1=Zoology
ColSubDepartment:1=LS Mammals
ColRecordType:1=Mammal Group Part
GeneralCatalogueNumber:1=irn: 9547055
EntIdeQualifiedNameAutomatic:1=Yes
PrtType:1=Skull
RegRegistrationParentRef:1={SAMPLE_SPECIMEN_ID}
RegRegistrationParentRefLocal:1={SAMPLE_SPECIMEN_ID}
RegRegistrationNumberLocal:1=ZD 1887.2.10.1
EntIdeFiledAsQualifiedNameLocal:1=Bos javanicus javanicus d'Alton, 1823
RegRegParentHigherTaxonLocal:1=Animalia; Chordata; Vertebrata; Mammalia; Artiodactyla; Bovidae; Bovinae
RegRegParentScientificNameLocal:1=Bos javanicus javanicus d'Alton, 1823
CatKindOfObject:1=Skull
LocCurrentLocationRef:1=264458
LocCurrentLocationRefLocal:1=264458
LocCurrentSummaryData:1=G1; Wandsworth; Wandsworth; B; WA.B.04; 5; 8
LocCurrentLocationLevel1Local:1=Wandsworth
LocCurrentLocationLevel2Local:1=Wandsworth
LocCurrentLocationLevel3Local:1=B
LocCurrentLocationLevel6Local:1=5
LocCurrentLocationLevel7Local:1=8
LocDateMoved=2022-02-07
LocTimeMoved=17:46:
LocIndependentlyMoveable:1=Yes
LocPermanentLocationRef:1=264457
LocPermanentLocationRefLocal:1=264457
LocPermanentSummaryData:1=G1; Wandsworth; Wandsworth; B; WA.B.04; 5; 7
LocPermLocationLevel7Local:1=7
ValDateValued=2022-02-07
ValValuationAmount:1=10.00
ValCurrency:1=£
AdmPublishWebNoPasswordFlag:1=Y
AdmPublishWebNoPassword:1=Yes
AdmPublishWebPasswordFlag:1=Y
AdmPublishWebPassword:1=Yes
AdmGUIDPreferredType:1=UUID4
AdmGUIDPreferredValue:1=20c771b3-7e4d-42a8-a6e4-7a5a38b27e8e
AdmGUIDIsPreferred:1=Yes
AdmGUIDType:1=UUID4
AdmGUIDValue:1=20c771b3-7e4d-42a8-a6e4-7a5a38b27e8e
AdmInsertedBy:1=James Ayre
AdmDateInserted=2022-02-07
AdmImportIdentifier:1=Harwell Audit Mammal Parts
AdmTimeInserted=17:46:29.000
AdmSystemIdentifier:1=jamea-220207-1730
AdmModifiedBy:1=Joseph Deane
AdmDateModified=2023-12-01
AdmTimeModified=09:39:35.000
AdmDateRecordModified=2024-01-08
AdmTimeRecordModified=14:14:18.000
SecRecordStatus:1=Active
SecCanDisplay:1=Group Default
SecCanEdit:1=Group Default
SecCanDelete:1=Group Default
SecDepartment:1=Zoology
SecLookupRoot:1=Zoology
"""

SAMPLE_MAMMAL_PART_ID, SAMPLE_MAMMAL_PART_DATA = read_emu_extract(raw_data)
