use utf8;

package Icecat::Schema::Result::CategoryKeyword;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->table("category_keywords");
__PACKAGE__->add_columns(
    category_id => { data_type => "integer",    is_nullable   => 1 },
    langid      => { data_type => "integer",    default_value => 0 },
    keywords    => { data_type => "mediumtext", is_nullable   => 1 },
    id          => { data_type => "integer" },
);
__PACKAGE__->set_primary_key("id");
__PACKAGE__->add_unique_constraint( [ "langid", "category_id" ] );

__PACKAGE__->belongs_to(
    category => "Icecat::Schema::Result::Category",
    { 'foreign.catid' => 'self.category_id' }
);

__PACKAGE__->belongs_to(
    language => "Icecat::Schema::Result::Language",
    "langid"
);

1;
