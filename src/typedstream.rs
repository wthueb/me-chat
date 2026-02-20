use crabstep::{OutputData, PropertyIterator, deserializer::iter::Property};

pub fn as_nsstring<'a>(property: &'a mut Property<'a, 'a>) -> Option<&'a str> {
    if let Property::Group(group) = property {
        let mut iter = group.iter_mut();
        if let Some(Property::Object { name, data, .. }) = iter.next()
            && (*name == "NSString" || *name == "NSAttributedString" || *name == "NSMutableString")
            && let Some(Property::Group(prim)) = data.next()
            && let Some(Property::Primitive(OutputData::String(s))) = prim.first()
        {
            return Some(s);
        }
    }
    None
}

pub fn as_nsdictionary<'a>(
    property: &'a mut Property<'a, 'a>,
) -> Option<&'a mut PropertyIterator<'a, 'a>> {
    if let Property::Group(group) = property {
        let mut iter = group.iter_mut();
        if let Some(Property::Object { name, data, .. }) = iter.next()
            && *name == "NSDictionary"
        {
            return Some(data);
        }
    }

    None
}

pub fn as_signed_integer(property: &Property<'_, '_>) -> Option<i64> {
    if let Property::Group(group) = property {
        let mut iter = group.iter();
        let val = iter.next()?;
        if let Property::Primitive(OutputData::SignedInteger(value)) = val {
            Some(*value)
        } else if let Property::Object { name, data, .. } = val
            && *name == "NSNumber"
        {
            as_signed_integer(&data.clone().next()?)
        } else {
            None
        }
    } else {
        None
    }
}
