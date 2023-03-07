
class ExtractorInstructionData:
    def __init__(self, ent_mapping , grp_field):
        self.entity_mapping = ent_mapping
        self.grouping_field = grp_field
        self.fixed_length_fields = []
        self.group_subset_fields = []
        self.group_entity_name = None

    def __init__(self):
        self.entity_mapping = None
        self.grouping_field = None
        self.fixed_lenght_fields = []
        self.group_subset_fields = []
        self.group_entity_name = None

    def get_entity_mapping(self):
        return self.entity_mapping

    def set_entity_mapping(self,ent_mapping):
        self.entity_mapping = ent_mapping

    def get_grouping_field(self):
        return self.grouping_field

    def set_grouping_field(self,grp_field):
        self.grouping_field = grp_field

    def get_fixed_length_fields(self):
        return self.fixed_lenght_fields

    def set_fixed_length_fields(self,data_file_fields):
        self.fixed_lenght_fields = data_file_fields

    def get_group_subset_fields(self):
        return self.group_subset_fields

    def set_group_subset_fields(self,grp_subset_fields):
        self.group_subset_fields = grp_subset_fields

    def get_group_entity_name(self):
        return self.group_entity_name

    def set_group_entity_name(self,grp_entity_name):
        self.group_entity_name = grp_entity_name





