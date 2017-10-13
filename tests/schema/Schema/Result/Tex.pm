use utf8;

package Icecat::Schema::Result::Tex;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->table("tex");
__PACKAGE__->add_columns(
    tex_id => { data_type => "integer" },
    tid    => { data_type => "integer" },
    langid => { data_type => "integer", default_value => 0 },
    value  => { data_type => "mediumtext" },
);
__PACKAGE__->set_primary_key("tex_id");
__PACKAGE__->add_unique_constraint( [ "tid", "langid" ] );

__PACKAGE__->belongs_to(
    language => "Icecat::Schema::Result::Language",
    "langid"
);

__PACKAGE__->belongs_to(
    tidindex => "Icecat::Schema::Result::TidIndex",
    "tid"
);

1;
