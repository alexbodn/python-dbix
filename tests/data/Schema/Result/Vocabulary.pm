use utf8;

package Icecat::Schema::Result::Vocabulary;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );
__PACKAGE__->table("vocabulary");
__PACKAGE__->add_columns(
    record_id => { data_type => "integer" },
    sid       => { data_type => "integer" },
    langid    => { data_type => "integer", default_value => 0 },
    value     => { data_type => "varchar", is_nullable => 1, size => 255 },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
);
__PACKAGE__->set_primary_key("record_id");
__PACKAGE__->add_unique_constraint( [ "sid", "langid" ] );

__PACKAGE__->belongs_to(
    language => "Icecat::Schema::Result::Language",
    "langid"
);

__PACKAGE__->belongs_to(
    sidindex => "Icecat::Schema::Result::SidIndex",
    "sid"
);

1;
