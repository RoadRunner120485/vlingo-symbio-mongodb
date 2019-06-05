// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.mongodb.journal;

import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.journal.JournalListener;
import org.bson.Document;

import java.util.List;

/**
 * All dispatching to this `JournalListener` are ignored.
 */
public class BaseMongoJournalListener implements JournalListener<Document> {
  /**
   * @see JournalListener#appended(Entry)
   */
  @Override
  public void appended(final Entry<Document> entry) { }

  /**
   * @see JournalListener#appendedWith(Entry, State)
   */
  @Override
  public void appendedWith(final Entry<Document> entry, final State<Document> snapshot) { }

  /**
   * @see JournalListener#appendedAll(List)
   */
  @Override
  public void appendedAll(final List<Entry<Document>> entries) { }

  /**
   * @see JournalListener#appendedAllWith(List, State)
   */
  @Override
  public void appendedAllWith(final List<Entry<Document>> entries, final State<Document> snapshot) { }
}
