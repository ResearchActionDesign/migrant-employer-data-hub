import re
from datetime import date, datetime, time
from typing import TYPE_CHECKING, List, Optional

import sqlalchemy as sa
from pydantic import AnyHttpUrl, condecimal, conint, constr
from sqlmodel import Field, Relationship

from constants import US_STATES_TO_ABBREV
from models.base import CaseStatus, DoLDataItem, DoLDataSource, clean_string_field
from models.dol_disclosure_job_order_address_record_link import (
    DolDisclosureJobOrderAddressRecordLink,
)
from models.employer_record import EmployerRecord

# Technique to avoid circular imports, see https://sqlmodel.tiangolo.com/tutorial/code-structure/
if TYPE_CHECKING:
    from models.address_record import AddressRecord


class DolDisclosureJobOrder(DoLDataItem, table=True):
    # Relationship fields
    employer_record_id: Optional[int] = Field(
        default=None, foreign_key="employer_record.id"
    )
    employer_record: Optional[EmployerRecord] = Relationship(
        back_populates="dol_disclosure_job_orders"
    )
    address_records: List["AddressRecord"] = Relationship(  # noqa
        back_populates="dol_disclosure_job_orders",
        link_model=DolDisclosureJobOrderAddressRecordLink,
    )

    file_name: Optional[str]
    file_row: Optional[int]

    # Override parent fields
    first_seen: Optional[datetime] = Field(sa_column=sa.Column(sa.DateTime))
    last_seen: Optional[datetime] = Field(
        sa_column=sa.Column(sa.DateTime, onupdate=datetime.utcnow)
    )
    source = Field(default=DoLDataSource.dol_disclosure)

    # Fields from the DoL Spreadsheet
    case_number: Optional[str] = Field(index=True)
    case_status: Optional[CaseStatus]
    received_date: Optional[datetime]
    decision_date: Optional[date]
    type_of_employer_application: Optional[str]
    h2a_labor_contractor: Optional[bool]
    nature_of_temporary_need: Optional[str]
    emergency_filing: Optional[bool]
    employer_name: Optional[str]
    trade_name_dba: Optional[str]
    employer_address_1: Optional[str]
    employer_address_2: Optional[str]
    employer_city: Optional[str]
    employer_state: Optional[str]
    employer_postal_code: Optional[constr(max_length=10)]
    employer_country: Optional[str]
    employer_province: Optional[str]
    employer_phone: Optional[str]
    employer_phone_ext: Optional[str]
    naics_code: Optional[str]
    employer_poc_last_name: Optional[str]
    employer_poc_first_name: Optional[str]
    employer_poc_middle_name: Optional[str]
    employer_poc_job_title: Optional[str]
    employer_poc_address1: Optional[str]
    employer_poc_address2: Optional[str]
    employer_poc_city: Optional[str]
    employer_poc_state: Optional[str]
    employer_poc_postal_code: Optional[constr(max_length=10)]
    employer_poc_country: Optional[str]
    employer_poc_province: Optional[str]
    employer_poc_phone: Optional[str]
    employer_poc_phone_ext: Optional[str]
    employer_poc_email: Optional[str]
    type_of_representation: Optional[str]
    attorney_agent_last_name: Optional[str]
    attorney_agent_first_name: Optional[str]
    attorney_agent_middle_name: Optional[str]
    attorney_agent_address_1: Optional[str]
    attorney_agent_address_2: Optional[str]
    attorney_agent_city: Optional[str]
    attorney_agent_state: Optional[str]
    attorney_agent_postal_code: Optional[constr(max_length=10)]
    attorney_agent_country: Optional[str]
    attorney_agent_province: Optional[str]
    attorney_agent_phone: Optional[str]
    attorney_agent_phone_ext: Optional[str]
    attorney_agent_email_address: Optional[str]
    lawfirm_name_business_name: Optional[str]
    state_of_highest_court: Optional[str]
    name_of_highest_state_court: Optional[str]
    soc_code: Optional[str]
    soc_title: Optional[str]
    seven_ninety_a_addendum_b_attached: Optional[bool]
    work_contracts_attached: Optional[bool]
    employer_mspa_attached: Optional[bool]
    surety_bond_attached: Optional[bool]
    housing_transportation: Optional[bool]
    appendix_a_attached: Optional[bool]
    jnt_employer_append_a_attached: Optional[bool]
    preparer_last_name: Optional[str]
    preparer_first_name: Optional[str]
    preparer_middle_initial: Optional[str]
    preparer_business_name: Optional[str]
    preparer_email: Optional[str]
    job_order_number: Optional[str]
    job_title: Optional[str]
    total_workers_needed: Optional[str]
    total_workers_h2a_requested: Optional[str]
    total_workers_h2a_certified: Optional[str]
    requested_begin_date: Optional[date]
    requested_end_date: Optional[date]
    employment_begin_date: Optional[date]
    employment_end_date: Optional[date]
    on_call_requirement: Optional[str]
    anticipated_number_of_hours: Optional[bool]
    sunday_hours: Optional[condecimal(ge=0, decimal_places=2)]
    monday_hours: Optional[condecimal(ge=0, decimal_places=2)]
    tuesday_hours: Optional[condecimal(ge=0, decimal_places=2)]
    wednesday_hours: Optional[condecimal(ge=0, decimal_places=2)]
    thursday_hours: Optional[condecimal(ge=0, decimal_places=2)]
    friday_hours: Optional[condecimal(ge=0, decimal_places=2)]
    saturday_hours: Optional[condecimal(ge=0, decimal_places=2)]
    hourly_schedule_begin: Optional[time]
    hourly_schedule_end: Optional[time]
    wage_offer: Optional[condecimal(ge=0, decimal_places=2)]
    per: Optional[str]
    piece_rate_offer: Optional[condecimal(ge=0, decimal_places=2)]
    piece_rate_unit: Optional[str]
    seven_ninenty_a_addendum_a_attached: Optional[bool]
    frequency_of_pay: Optional[str]
    other_frequency_of_pay: Optional[str]
    deductions_from_pay: Optional[str]
    education_level: Optional[str]
    work_experience_months: Optional[conint(ge=0)]
    training_months: Optional[conint(ge=0)]
    certification_requirements: Optional[bool]
    driver_requirements: Optional[bool]
    criminal_background_check: Optional[bool]
    drug_screen: Optional[bool]
    lifting_requirements: Optional[bool]
    lifting_amount: Optional[conint(ge=0)]
    exposure_to_temperatures: Optional[bool]
    extensive_pushing_pulling: Optional[bool]
    extensive_sitting_walking: Optional[str]
    frequent_stooping_bending_over: Optional[bool]
    repetitive_movements: Optional[bool]
    supervise_other_emp: Optional[bool]
    supervise_how_many: Optional[conint(ge=0)]
    special_requirements: Optional[str]
    worksite_address: Optional[str]
    worksite_city: Optional[str]
    worksite_state: Optional[str]
    worksite_postal_code: Optional[constr(max_length=10)]
    worksite_county: Optional[str]
    addendum_b_worksite_attached: Optional[bool]
    total_worksites_records: Optional[str]
    housing_address_location: Optional[str]
    housing_city: Optional[str]
    housing_state: Optional[str]
    housing_postal_code: Optional[constr(max_length=10)]
    housing_county: Optional[str]
    type_of_housing: Optional[str]
    total_units: Optional[conint(ge=0)]
    total_occupancy: Optional[conint(ge=0)]
    housing_compliance_local: Optional[bool]
    housing_compliance_state: Optional[bool]
    housing_compliance_federal: Optional[bool]
    addendum_b_housing_attached: Optional[bool]
    total_housing_records: Optional[conint(ge=0)]
    meals_provided: Optional[bool]
    meals_charge: Optional[condecimal(ge=0, decimal_places=2)]
    meal_reimbursement_minimum: Optional[condecimal(ge=0, decimal_places=2)]
    meal_reimbursement_maximum: Optional[condecimal(ge=0, decimal_places=2)]
    phone_to_apply: Optional[str]
    email_to_apply: Optional[str]
    website_to_apply: Optional[AnyHttpUrl]
    addendum_c_attached: Optional[bool]
    total_addendum_a_records: Optional[conint(ge=0)]

    def clean(self):
        if self.trade_name_dba:
            self.trade_name_dba = re.sub(
                r"^(DBA|BDA|dba|dba:|d\/b\/a) ", "", self.trade_name_dba
            )

        fields_to_strip = (
            "employer_name",
            "employer_address_1",
            "employer_address_2",
            "employer_city",
            "employer_state",
            "employer_country",
            "employer_postal_code",
            "trade_name_dba",
        )

        for c in fields_to_strip:
            setattr(self, c, clean_string_field(getattr(self, c)))

        if str(self.employer_state).lower() in US_STATES_TO_ABBREV:
            self.employer_state = US_STATES_TO_ABBREV[
                str(self.employer_state).lower()
            ].upper()

        return self
