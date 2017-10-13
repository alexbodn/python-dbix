use utf8;

package Icecat::Schema::Result::ProductMultimediaObject;

=head1 NAME

Icecat::Schema::Result::ProductMultimediaObject

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

product_multimedia_object

=cut

__PACKAGE__->table("product_multimedia_object");
__PACKAGE__->add_columns(
    id => {
        data_type         => "integer",
        is_auto_increment => 1
    },
    product_id  => { data_type => "integer" },
    link        => { data_type => "varchar", size => 255 },
    description => { data_type => "mediumtext" },
    langid      => { data_type => "integer" },
    size        => { data_type => "integer" },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
    content_type   => { data_type => "varchar", size => 255 },
    keep_as_url    => { data_type => "integer" },
    type           => { data_type => "varchar", size => 255 },
    height         => { data_type => "integer" },
    width          => { data_type => "integer" },
    is_rich        => { data_type => "boolean" },
    preview_height => { data_type => "integer" },
    preview_size   => { data_type => "integer" },
    preview_width  => { data_type => "integer" },
    preview_url    => { data_type => "varchar", size => 255 },
    source         => { data_type => "varchar", size => 255 },
    thumb_url      => { data_type => "varchar", size => 255 },
    uuid           => { data_type => "varchar", size => 40 },
);
__PACKAGE__->set_primary_key("id");
__PACKAGE__->add_unique_constraint( [ "product_id", "langid", "link" ] );

__PACKAGE__->belongs_to(
    product => "Icecat::Schema::Result::Product",
    "product_id"
);

__PACKAGE__->belongs_to(
    language => "Icecat::Schema::Result::Language",
    "langid"
);

1;
