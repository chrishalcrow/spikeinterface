from .center_of_mass import LocalizeCenterOfMass
from .monopolar import LocalizeMonopolarTriangulation
from .grid import LocalizeGridConvolution
from .dartsort_lozalization import DartSortLocalize

_methods_list = [LocalizeCenterOfMass, LocalizeMonopolarTriangulation, LocalizeGridConvolution, DartSortLocalize]
peak_localization_methods = {m.name: m for m in _methods_list}
