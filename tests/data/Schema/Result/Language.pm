use utf8;

package Icecat::Schema::Result::Language;

=head1 NAME

Icecat::Schema::Result::Language

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

language

=cut

__PACKAGE__->table("language");
__PACKAGE__->add_columns(
    langid        => { data_type => "integer" },
    sid           => { data_type => "integer" },
    code          => { data_type => "varchar", size => 32 },
    short_code    => { data_type => "varchar", size => 5 },
    published     => { data_type => "boolean", default_value => "1" },
    backup_langid => { data_type => "integer", is_nullable => 1 },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
);

__PACKAGE__->set_primary_key("langid");

__PACKAGE__->add_unique_constraint(['code']);

__PACKAGE__->add_unique_constraint(['short_code']);

__PACKAGE__->belongs_to(
    backup_language => "Icecat::Schema::Result::Language",
    { 'foreign.langid' => 'self.backup_langid' }, 
    { join_type => "left" }
);

__PACKAGE__->has_many(
    category_keywords => "Icecat::Schema::Result::CategoryKeyword",
    "langid"
);

__PACKAGE__->has_many(
    measure_signs => "Icecat::Schema::Result::MeasureSign",
    "langid"
);

__PACKAGE__->has_many(
    product_descriptions => "Icecat::Schema::Result::ProductDescription",
    "langid"
);

__PACKAGE__->has_many(
    product_feature_locals => "Icecat::Schema::Result::ProductFeatureLocal",
    "langid"
);

__PACKAGE__->has_many(
    product_galleries => "Icecat::Schema::Result::ProductGallery",
    "langid"
);

__PACKAGE__->has_many(
    product_multimedia_objects =>
      "Icecat::Schema::Result::ProductMultimediaObject",
    "langid"
);

__PACKAGE__->has_many(
    product_reviews => "Icecat::Schema::Result::ProductReview",
    "langid"
);

__PACKAGE__->has_many(
    names => "Icecat::Schema::Result::Vocabulary",
    { 'foreign.sid' => 'self.sid' }
);

__PACKAGE__->belongs_to(
    sidindex => "Icecat::Schema::Result::SidIndex",
    "sid"
);
__PACKAGE__->has_many(
    texts => "Icecat::Schema::Result::Tex",
    "langid"
);

__PACKAGE__->has_many(
    vocabularies => "Icecat::Schema::Result::Vocabulary",
    "langid"
);

1;
