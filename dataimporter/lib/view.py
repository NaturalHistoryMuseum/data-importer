from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Iterable, List, Optional, Union

from splitgill.utils import partition

from dataimporter.lib.dbs import ChangeQueue, Index, Store
from dataimporter.lib.model import SourceRecord


@dataclass
class FilterResult:
    """
    Class representing the result of filtering a record in a view.

    We could use bools for this but having a class allows us to associate details too
    which is helpful for debugging why something is or isn't published on the Portal for
    example.
    """

    success: bool
    reason: Optional[str] = None

    def __bool__(self):
        return self.success


# reusable success FilterResult object
SUCCESS_RESULT = FilterResult(True)


def strip_empty(func):
    """
    A decorator which strips out keys that have empty strings from the dict returned
    from the decorated function.

    :param func: the function to decorate which returns a dict
    :return: a decorated function
    """

    @wraps(func)
    def inner(*args, **kwargs):
        return {
            key: value
            for key, value in func(*args, **kwargs).items()
            if value is not None
        }

    return inner


class View:
    """
    A view on a data DB which provides a few different things, primarily:

    - filtering to determine what counts as a member of this view
    - transformation to convert a data DB SourceRecord object into a publishable
      record
    - tracking of records that change in the data DB that are members of this view
    - links from this view to other views
    """

    def __init__(self, path: Path, store: Store, published_name: Optional[str] = None):
        """
        :param path: the root path that all view related data should be stored under
        :param store: the Store object that backs this view
        :param published_name: the name of the SplitgillDatabase this view populates
        """
        self.path = path
        self.store = store
        self.published_name = published_name
        self.name = path.name
        self.changes = ChangeQueue(self.path / 'changes')
        # a list of links which need to be updated when records in this view change
        self.dependants: List[Link] = []

    @property
    def is_published(self) -> bool:
        """
        Returns whether this view directly feeds a Splitgill database, i.e. is
        "published" to the Portal.

        :return: True if a Splitgill database should exist for this view, False if not
        """
        return self.published_name is not None

    def add_dependant(self, link: 'Link'):
        """
        Add a dependant via the given link.

        :param link: the Link to add
        """
        self.dependants.append(link)

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Given a record, determine whether it is a member of this view. Return a
        FilterResult with a success=True attribute if yes, otherwise return a
        FilterResult with a success=False attribute and a reason describing why the
        record isn't a member of this view.

        By default, this function returns a FilterResult with success=True.

        :param record: the SourceRecord to check
        :return: a FilterResult object
        """
        return SUCCESS_RESULT

    def is_publishable(self, record: SourceRecord) -> FilterResult:
        """
        Given a record, determine whether it is published based on the rules of this
        view. Return a FilterResult with a success=True attribute if yes, otherwise
        return a FilterResult with a success=False attribute and a reason describing why
        the record should not be published.

        By default, this function returns a FilterResult with success=True.

        :param record: the SourceRecord to check
        :return: a FilterResult object
        """
        return SUCCESS_RESULT

    def is_publishable_member(self, record: SourceRecord) -> FilterResult:
        """
        Given a record, determine whether it is first a member of this view and then is
        publishable under the rules of this view. This functionality was previously
        entirely contained within is_member, but has since been split into separate
        methods.

        :param record: the SourceRecord to check
        :return: a FilterResult object
        """
        check_member = self.is_member(record)
        if not check_member:
            return check_member
        return self.is_publishable(record)

    def transform(self, record: SourceRecord) -> dict:
        """
        Given a record, return a dict which will be ingested by Splitgill. This is
        therefore likely to be the representation that makes it to the Data Portal. This
        representation may include any data from linked views.

        :param record: a SourceRecord object
        :return: a dict
        """
        return record.data

    def find(self, ids: Iterable[str]) -> Iterable[SourceRecord]:
        """
        Find all records matching the given ids and yield them. Records which are
        deleted or not members of this view are not returned, they are silently skipped.

        :param ids: an iterable of IDs
        :return: yields SourceRecord objects
        """
        yield from (
            record
            for record in self.store.get_records(ids, yield_deleted=False)
            if self.is_member(record)
        )

    def get(self, record_id: str) -> Optional[SourceRecord]:
        """
        Get and return the record with the given ID. If the record can't be found, is
        deleted, or isn't a member of this view, None is returned.

        :param record_id: the record ID
        :return: None or a SourceRecord object
        """
        record = self.store.get_record(record_id, return_deleted=False)
        if record is not None and self.is_member(record):
            return record
        return None

    def find_and_transform(self, ids: Iterable[str]) -> Iterable[dict]:
        """
        Convenience function which finds the records in the view's backing store which
        have the given IDs, passes them through the transform method, and then yields
        them.

        If any IDs aren't in this view, can't be found, are embargoed, or are deleted
        they will be ignored.

        :param ids: the ids to look up
        :return: an iterable of dicts
        """
        return map(self.transform, self.find(ids))

    def get_and_transform(self, record_id: str) -> Optional[dict]:
        """
        Convenience function which finds the record in this view with the given ID,
        passes it to the transform method, and then returns the result. If the record
        can't be found, aren't members of this view, are embargoed, or deleted, then
        None is returned and transform isn't called.

        :param record_id: the record ID to look up and transform
        :return: the data dict from the transform call or None
        """
        record = self.get(record_id)
        return None if record is None else self.transform(record)

    def queue(self, records: List[SourceRecord]):
        """
        Given a set of records, update the changed records queue in this view. This will
        add any changed records to the queue, as well as any deletions.

        This method doesn't account for records that have changed view. I.e. were a
        member of this view and then changed to become a member of a different view.
        This could be accounted for but given how often this is likely to happen, it's a
        big performance penalty to deal with it when it is really unlikely to happen.

        :param records: the records to check and queue
        """
        changed_records = [
            record
            for record in records
            # filter out records that have already been queued (this is inefficient, but
            # also we need this condition to prevent infinite loops on views linked to
            # themselves)
            if not self.changes.is_queued(record.id)
            and (record.is_deleted or self.is_member(record))
        ]
        if not changed_records:
            return

        self.changes.put_many(changed_records)

        # loop through all the views that link to this one and update their queues too
        for link in self.dependants:
            link.queue_changes(changed_records)

    def count(self) -> int:
        """
        Count the number of record IDs in this view's change queue.

        :return: the number of IDs in the change queue
        """
        return self.changes.size()

    def iter_changed(self) -> Iterable[SourceRecord]:
        """
        Iterate over the IDs in the change queue and yield the corresponding
        SourceRecord objects.

        :return: yields SourceRecord objects
        """
        yield from self.store.get_records(self.changes.iter(), yield_deleted=True)

    def iter_all(self) -> Iterable[SourceRecord]:
        """
        Iterate over all records in the view, yielding them as SourceRecord objects.
        Only records which are members of this view or have been deleted are yielded.

        :return: yields SourceRecord objects
        """
        # this can produce deletes for records that may never have been members and
        # therefore never made it to mongo, but that's ok, we'll ignore them when
        # we try to add them to mongo and find that they don't exist already
        yield from (
            record
            for record in self.store.iter(yield_deleted=True)
            if record.is_deleted or self.is_member(record)
        )

    def flush(self):
        """
        Remove all IDs from the main view queue.
        """
        self.changes.clear()

    def rebuild(self):
        """
        Remove all data and then reread the entire source database to rebuild it.
        """
        # clear everything out
        self.changes.clear()
        # reload from source
        for chunk in partition(self.store.iter(yield_deleted=False), 5000):
            self.queue(chunk)

    def close(self):
        """
        Close the changes database.

        Note that this method doesn't close the underlying store because it could be
        being used by other views.
        """
        self.changes.close()


@dataclass
class Ref:
    """
    Class representing a field which has values that reference another view.
    """

    field: Optional[str] = None

    @property
    def is_id(self) -> bool:
        """
        Returns True if the field is an ID field.

        :return: True if is an ID, False if not
        """
        return self.field is None

    def get_values(self, record: SourceRecord) -> List[str]:
        """
        Returns all the values in this field from the given record.

        :param record: the record to get the values from
        :return: the values in this field as a list (even if there's only one value)
        """
        if self.field is None:
            return [record.id]
        else:
            return list(record.iter_all_values(self.field))

    def get_value(self, record: SourceRecord) -> Optional[str]:
        """
        Returns the first value in this field from the given record. If no values exist,
        returns None.

        :param record: the record to get the value from
        :return: the value or None
        """
        if self.field is None:
            return record.id
        else:
            return next(iter(record.iter_all_values(self.field)), None)


# reusable ID reference
ID = Ref()


@dataclass
class Link:
    """
    Represents a link from one view to another view.

    The views can be the same view, creating a self-referential link.
    """

    owner: View
    owner_ref: Ref
    owner_index: Optional[Index]
    foreign: View
    foreign_ref: Ref
    foreign_index: Optional[Index]

    def queue_changes(self, foreign_records: List[SourceRecord]):
        """
        Given a list of foreign records which have changed, looks up the owner records
        which relate to them, and queues those records on the owner view.

        :param foreign_records: the foreign records that have changed
        """
        ids_to_queue = []

        for foreign_record in foreign_records:
            # get the ref values from the record
            ref_values = self.foreign_ref.get_values(foreign_record)
            if not ref_values:
                continue

            if self.owner_ref.is_id:
                # ref values are IDs, so we can just use them immediately
                ids_to_queue.extend(ref_values)
            else:
                # ref values are an intermediary, need to look this up in the index
                ids_to_queue.extend(self.owner_index.lookup(ref_values))

        if ids_to_queue:
            self.owner.queue(list(self.owner.find(ids_to_queue)))

    def lookup(self, owner_record: SourceRecord) -> List[SourceRecord]:
        """
        Lookup all the foreign records related to the given owner record.

        :param owner_record: a record from the owner view
        :return: a list of foreign records
        """
        # get the ref values from the record
        ref_values = self.owner_ref.get_values(owner_record)
        if not ref_values:
            return []

        if self.foreign_ref.is_id:
            # ref values are IDs, so we can just use them immediately
            ref_ids = ref_values
        else:
            # ref values are an intermediary, need to look this up in the index
            ref_ids = list(self.foreign_index.lookup(ref_values))
            if not ref_ids:
                return []

        # return the records that are found in this view
        return list(self.foreign.find(ref_ids))

    def lookup_one(self, owner_record: SourceRecord) -> Optional[SourceRecord]:
        """
        Lookup the first foreign record related to the given owner record.

        :param owner_record: a record from the owner view
        :return: a foreign record or None if no records are related
        """
        return next(iter(self.lookup(owner_record)), None)

    def lookup_and_transform(self, owner_record: SourceRecord) -> List[dict]:
        """
        Lookup all the foreign records related to the given owner record, transform
        them, and return the resulting list of dicts.

        :param owner_record: a record from the owner view
        :return: a list of dicts created from the related foreign records
        """
        return [self.foreign.transform(record) for record in self.lookup(owner_record)]

    def lookup_and_transform_one(self, owner_record: SourceRecord) -> Optional[dict]:
        """
        Lookup the first foreign record related to the given owner record, transform it,
        and return the resulting dict.

        :return: a foreign record or None if no records are related
        :return: a dict created from the related foreign record, or None if no records
            are related
        """
        return next(iter(self.lookup_and_transform(owner_record)), None)


def make_link(
    owner: View,
    owner_ref: Union[str, Ref],
    foreign: View,
    foreign_ref: Union[str, Ref],
) -> Link:
    """
    Create a link between two views. This essentially creates a many-to-many link
    between the two views allowing lookup of records from the owner to the foreign view
    as part of record transformation, and the flow of updates from the foreign view back
    to the owner view when linked records change. Links can be self-referential.

    If any index are required for the link to work and data already exists in the
    backing stores, they will be created meaning this function may take time to
    complete. If the indexes already exist, they will not be recreated.

    :param owner: the view that is creating this link and will have updates queued upon
        it when changes occur in the foreign view
    :param owner_ref: the field containing the values referencing the foreign view
    :param foreign: the view that is being linked to, the owner view will be added to
        this view as a dependant
    :param foreign_ref: the field containing the values referenced by the owner view
    :return: a Link object
    """
    # ensure the refs are Refs
    if isinstance(owner_ref, str):
        owner_ref = Ref(owner_ref)
    if isinstance(foreign_ref, str):
        foreign_ref = Ref(foreign_ref)

    # create indexes as needed
    if owner_ref.is_id:
        owner_index = None
    else:
        owner_index = owner.store.create_index(owner_ref.field)
    if foreign_ref.is_id:
        foreign_index = None
    else:
        foreign_index = foreign.store.create_index(foreign_ref.field)

    link = Link(owner, owner_ref, owner_index, foreign, foreign_ref, foreign_index)
    foreign.add_dependant(link)
    return link
