use utf8;

package Icecat::Schema::Result::ProductDescription;

=head1 NAME

Icecat::Schema::Result::ProductDescription

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

product_description

=cut

__PACKAGE__->table("product_description");
__PACKAGE__->add_columns(
    product_description_id =>
      { data_type => "integer", is_auto_increment => 1 },
    product_id => { data_type => "integer" },
    langid     => { data_type => "integer" },
    short_desc => { data_type => "varchar", default_value => "", size => 3000 },
    long_desc  => { data_type => "mediumtext" },
    specs_url  => { data_type => "varchar", default_value => "", size => 512 },
    support_url => { data_type => "varchar", default_value => "", size => 255 },
    official_url   => { data_type => "text",       is_nullable => 1 },
    warranty_info  => { data_type => "mediumtext", is_nullable => 1 },
    option_field_1 => { data_type => "mediumtext", is_nullable => 1 },
    updated        => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
    pdf_url => { data_type => "varchar", default_value => "", size => 255 },
    option_field_2 => { data_type => "mediumtext", is_nullable => 1 },
    pdf_size =>
      { data_type => "integer", default_value => 0, is_nullable => 1 },
    manual_pdf_url =>
      { data_type => "varchar", default_value => "", size => 255 },
    manual_pdf_size =>
      { data_type => "integer", default_value => 0, is_nullable => 1 },
    pdf_url_origin        => { data_type => "text",    is_nullable   => 1 },
    manual_pdf_url_origin => { data_type => "text",    is_nullable   => 1 },
    pdf_updated           => { data_type => "integer", default_value => 0 },
    manual_pdf_updated    => { data_type => "integer", default_value => 0 },
);
__PACKAGE__->set_primary_key("product_description_id");
__PACKAGE__->add_unique_constraint( [ "product_id", "langid" ] );

__PACKAGE__->belongs_to(
    product => "Icecat::Schema::Result::Product",
    "product_id"
);

__PACKAGE__->belongs_to(
    language => "Icecat::Schema::Result::Language",
    "langid"
);

1;
