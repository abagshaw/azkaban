/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package azkaban.project;

import azkaban.spi.Storage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.mockito.Mockito.*;


public class ArchiveUnthinnerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private final int ID = 107;
  private final int VERSION = 10;
  private final Project project = new Project(this.ID, "project1");

  private ArchiveUnthinner archiveUnthinner;
  private Storage storage;

  @Before
  public void setUp() throws Exception {
    this.storage = mock(Storage.class);

    this.archiveUnthinner = new ArchiveUnthinner(this.storage);
  }

  @Test
  public void freshValidProject() {

  }
}
