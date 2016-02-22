package com.javaninja.batch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.StepScopeTestExecutionListener;
import org.springframework.batch.test.StepScopeTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author norris.shelton
 */
@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({DependencyInjectionTestExecutionListener.class,
                         StepScopeTestExecutionListener.class,
                         TransactionalTestExecutionListener.class})
@Transactional
@ContextConfiguration
public class TestReaderAndWriter {

    @Autowired
    private JpaPagingItemReader<CamAffiliateEntity> itemReader;

    @Autowired
    private JpaItemWriter<CamAffiliateEntity> itemWriter;

    @Test
    public void testReader() {
        StepExecution execution = MetaDataInstanceFactory.createStepExecution();
        int count = 0;
        try {
            count = StepScopeTestUtils.doInStepScope(execution, () -> {
                int numStates = 0;
                itemReader.open(execution.getExecutionContext());
                CamAffiliateEntity camAffiliateEntity;
                try {
                    while ((camAffiliateEntity = itemReader.read()) != null) {
                        assertNotNull(camAffiliateEntity);
                        assertNotNull(camAffiliateEntity.getAffiliateId());
                        assertNotNull(camAffiliateEntity.getName());
                        assertNotNull(camAffiliateEntity.getChannelId());
                        numStates++;
                    }
                } finally {
                    try { itemReader.close(); } catch (ItemStreamException e) { fail(e.toString());
                    }
                }
                return numStates;
            });
        } catch (Exception e) {
            fail(e.toString());
        }
        assertEquals(12, count);
    }

    @Test
    public void testWriter() throws Exception {
        List<CamAffiliateEntity> usStateEntities = new LinkedList<>();
        CamAffiliateEntity usStateEntity;
        for (int i = 0; i < 100; i++) {
            usStateEntity = new CamAffiliateEntity();
            usStateEntity.setAffiliateId(i);
            usStateEntity.setName("TEST-DELETE-" + i);
            usStateEntity.setChannelId(13);  // test
            usStateEntities.add(usStateEntity);
        }

        StepExecution execution = MetaDataInstanceFactory.createStepExecution();
        StepScopeTestUtils.doInStepScope(execution, () -> {
            itemWriter.write(usStateEntities);
            return null;
        });
    }
}
